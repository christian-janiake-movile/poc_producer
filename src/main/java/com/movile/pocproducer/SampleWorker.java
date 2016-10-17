package com.movile.pocproducer;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.movile.pgle.PeerGroup;
import com.movile.pgle.listener.Worker;
import com.movile.res.redis.RedisConnectionManager;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component("sampleWorker")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SampleWorker implements Worker, Runnable {

    private PeerGroup peerGroup;
    private Instant executionStart;

    private Date lastDatabaseQuery;
    private AtomicInteger lastRowRead = new AtomicInteger(0);

    private Integer carrierId;              // carrier id used on query to fetch users
    private String paginationTable;         // name of table used to paginate the results
    private Integer refreshIntervalMinutes; // interval pagination table is generated
    private Integer forceRefresIdleMinutes; // force refresh if idle for some minutes; default is 15 minutes
    private Integer usersPerCycle;          // users selected on each cycle
    private Integer cycleDurationInSeconds; // if positive value, each cycle should take at least this duration;
                                            // if zero or negative, code runs only once each time a leader is selected
                                            // default is 60 seconds
    private Integer usersBatchSize;         // size of each batch to be submmited to queue; default is 100 users
    private Long forceRefreshIdleTime;

    @Autowired
    RedisConnectionManager redis;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    Logger log;

    @Autowired
    private ApplicationContext ctx;

    @Autowired
    private Destination destination;

    private JmsTemplate jmsTemplate;

    SecurityCache securityCache;

    @Value("#{systemProperties['redis.sbs.engine.key.prefix'] ?: 'sbs.engine'}")
    private String keyPrefix;

    private String masterQueryCommand;
    private String loadBatchQuery;
    private int offset = 0;

    public SampleWorker(PeerGroup peerGroup) {
        this.peerGroup = peerGroup;

        Validate.isTrue(peerGroup.getProperties().containsKey("engine.carrierId"), "Invalid value for engine.carrierId on peerGroup " + peerGroup.getName());
        Validate.isTrue(peerGroup.getProperties().containsKey("engine.paginationTable"), "Invalid value for engine.paginationTable on peerGroup " + peerGroup.getName());
        Validate.isTrue(peerGroup.getProperties().containsKey("engine.refreshIntervalMinutes"), "Invalid value for engine.refreshIntervalMinutes on peerGroup " + peerGroup.getName());
        Validate.isTrue(peerGroup.getProperties().containsKey("engine.usersPerCycle"), "Invalid value for engine.usersPerCycle on peerGroup " + peerGroup.getName());

        this.carrierId = Integer.parseInt(peerGroup.getProperties().getProperty("engine.carrierId"));
        this.paginationTable = peerGroup.getProperties().getProperty("engine.paginationTable");
        this.refreshIntervalMinutes = Integer.parseInt(peerGroup.getProperties().getProperty("engine.refreshIntervalMinutes"));
        this.forceRefresIdleMinutes = Integer.parseInt(peerGroup.getProperties().getProperty("engine.forceRefresIdleMinutes", "15"));
        this.usersPerCycle = Integer.parseInt(peerGroup.getProperties().getProperty("engine.usersPerCycle"));
        this.cycleDurationInSeconds = Integer.parseInt(peerGroup.getProperties().getProperty("engine.cycleDurationInSeconds", "60"));
        this.usersBatchSize = Integer.parseInt(peerGroup.getProperties().getProperty("engine.usersBatchSize", "100"));
    }

    public void work() throws InterruptedException {
        synchronized(this) {
            this.notify();
        }
    }

    @PostConstruct
    public void init() {
        securityCache = (SecurityCache) ctx.getBean("securityCache", carrierId);
        jmsTemplate = (JmsTemplate) ctx.getBean("jmsTemplate", "hybridChargeQueue_I1");
        loadJdbcStatements();
    }

    public void run() {

        while(true) {

            synchronized(this) {

                // wait until work() method allow execution using notify()
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            while(peerGroup.isLeader()) {
                executionStart = Instant.now();
                try {

                    // read & update pagination status on redis
                    String redisKey = paginationInfoKey();
                    Map<String, String> currentStatus = redis.hgetall(redisKey);

                    boolean refreshTable = false;

                    Long lastRefresh = null;
                    Integer totalRecords = null;

                    if (currentStatus != null && currentStatus.containsKey("lastRefresh")) {
                        log.info("Pagination info data: {}", currentStatus.toString());

                        lastRefresh = Long.parseLong(currentStatus.get("lastRefresh"));
                        totalRecords = new Integer(currentStatus.getOrDefault("totalRecords", "0"));
                        this.offset = new Integer(currentStatus.getOrDefault("offset", "0"));

                        if (lastRefresh + TimeUnit.MINUTES.toMillis(refreshIntervalMinutes) < System.currentTimeMillis()) {
                            log.info("Interval exceeded. Generate pagination table {}", paginationTable);
                            refreshTable = true;
                        } else if (!DateUtils.isSameDay(new Date(), new Date(lastRefresh))) {
                            log.info("Turn of the day. Generate pagination table {}", paginationTable);
                            refreshTable = true;
                        } else if (forceRefreshIdleTime != null && forceRefreshIdleTime.longValue() <= System.currentTimeMillis()) {
                            log.info("Producer is idle. Generate pagination table {}", paginationTable);
                            refreshTable = true;
                        }

                    } else {
                        log.info("First execution. Generate pagination table {}", paginationTable);
                        refreshTable = true;
                    }

                    if (refreshTable) {
                        lastRefresh = System.currentTimeMillis();
                        totalRecords = generatePaginationTable();
                        this.offset = 0;
                    }

                    int usersSubmitted = 0;
                    List<String> usersBatch = new LinkedList<String>();
                    BloomFilter<CharSequence> newBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), usersPerCycle);

                    Map<String, BloomFilter<CharSequence>> bloomFilters = securityCache.loadBloomFilters();

                    // submit users while records available and target number of users not reached
                    while (peerGroup.isLeader() && offset < totalRecords && usersSubmitted < usersPerCycle) {

                        // fetch records enough to reach target number of users
                        List<String> users = fetchUsers(usersPerCycle - usersSubmitted);
                        log.debug("{} users fetched", users.size());

                        // check if leadership ended during database access
                        if(!peerGroup.isLeader()) {
                            continue;
                        }

                        offset += users.size();
                        writePaginationStatus(redisKey, lastRefresh, totalRecords);

                        for (String msisdn : users) {

                            // filter users that have been processed recently
                            if (!securityCache.isOnCache(msisdn, bloomFilters)) {
                                log.debug("User {} submitted", msisdn);
                                newBloomFilter.put(msisdn);
                                securityCache.putOnSecurityCache(msisdn, executionStart.toEpochMilli());
                                usersBatch.add(msisdn);
                                usersSubmitted++;

                                // batch is complete: pack and ship
                                if (usersBatch.size() == usersBatchSize) {
                                    putOnProcessingQueue(usersBatch);
                                    usersBatch = new LinkedList<String>();
                                }
                            } else {
                                log.debug("User {} BLOCKED", msisdn);
                            }
                        }
                    }

                    log.info("User submission finished");

                    // pack and ship last batch
                    if (usersBatch.size() > 0) {
                        putOnProcessingQueue(usersBatch);
                    }

                    // store submitted users bloomfilter into sorted set
                    if (usersSubmitted > 0) {
                        log.info("Store new created bloomfilter under key {}", executionStart.toEpochMilli());
                        securityCache.writeBloomFilter(newBloomFilter, executionStart.toEpochMilli());
                    }

                    // if query results are exhausted, schedule a forced refresh
                    if (offset >= totalRecords && forceRefreshIdleTime == null && forceRefresIdleMinutes > 0) {
                        log.info("Engine is going to be idle for now on, force refresh in 10 minutes");
                        forceRefreshIdleTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(forceRefresIdleMinutes);
                    }

                } catch(Exception e) {
                    log.error("Error processing user selection", e);
                }

                // if duration of cycle is controlled, sleep until expected duration is reached
                if(cycleDurationInSeconds > 0) {
                    Instant wakeUp = executionStart.plus(cycleDurationInSeconds, ChronoUnit.SECONDS);
                    Duration sleepDuration = Duration.between(Instant.now(), wakeUp);
                    if (!sleepDuration.isNegative()) {
                        try {
                            log.info("Sleep {} seconds", sleepDuration.getSeconds());
                            Thread.sleep(sleepDuration.toMillis());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        log.info("Execution took {}s more than supposed", sleepDuration.getSeconds());
                    }
                } else {
                    // if duration of cycle is zero or negative, code runs only once each election
                    break;
                }
            }
        }
    }

    private void writePaginationStatus(String redisKey, Long lastRefresh, Integer totalRecords) {
        Map<String, String> currentStatus = new HashMap<String, String>();

        // update pagination status on redis
        currentStatus.put("lastRefresh", Long.toString(lastRefresh));
        currentStatus.put("offset", Integer.toString(offset));
        currentStatus.put("totalRecords", Integer.toString(totalRecords));
        redis.hmset(redisKey, currentStatus);

        log.info("Updated pagination info with data: {}", currentStatus.toString());
    }

    private void putOnProcessingQueue(List<String> usersList) {
        log.info("Put batch on queue with {} elements", usersList.size());

        jmsTemplate.send(new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(formatMessage(usersList));
            }});
    }

    private String formatMessage(List<String> usersList) {
        return "";
    }

    private Integer generatePaginationTable() {

        return jdbcTemplate.update(masterQueryCommand);
    }

    private List<String> fetchUsers(int numRecords) {
        log.info("Fetching {} records starting at #{}", numRecords, offset);
        List<String> result = jdbcTemplate.query(loadBatchQuery, new Object[] {offset, (offset + numRecords)}, new RowMapper() {

            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                String phone = rs.getString("phone");
                log.info("User {} loaded", phone);
                return phone;
            }
        });
        return result;
    }

    private String paginationInfoKey() {
        return keyPrefix + "." + this.peerGroup.getId().toString();
    }

    private void loadJdbcStatements() {
        StringBuilder masterQueryCommandBuilder = new StringBuilder();
        masterQueryCommandBuilder.append("INSERT INTO ").append(paginationTable);
        masterQueryCommandBuilder.append(" (phone, min_last_renew_attempt)");
        masterQueryCommandBuilder.append(" SELECT phone, MIN(s.last_renew_attempt) AS min_last_renew_attempt");
        masterQueryCommandBuilder.append(" FROM subscription AS s WITH(nolock)");
        masterQueryCommandBuilder.append("  JOIN configuration AS c WITH(nolock) ON s.configuration_id = c.configuration_id");
        masterQueryCommandBuilder.append("  JOIN lifecycle AS l WITH(nolock) ON l.lifecycle_id = s.current_lifecycle_id");
        masterQueryCommandBuilder.append("  WHERE DATEADD(day, l.[days], s.timeout_date) < GETDATE()");
        masterQueryCommandBuilder.append(" AND s.enabled = 1");
        masterQueryCommandBuilder.append(" AND s.related_id IS NULL");
        masterQueryCommandBuilder.append(" AND s.subscription_status_id IN (0,2)");
        masterQueryCommandBuilder.append(" AND c.carrier_id = " + carrierId.toString());
        masterQueryCommandBuilder.append(" AND ( c.enabled = 1 AND");
        masterQueryCommandBuilder.append(" ( c.renewable = 1 OR c.use_balance_parameter = 1 ) )");
        masterQueryCommandBuilder.append(" GROUP BY s.phone ");
        masterQueryCommandBuilder.append(" ORDER BY MAX(ISNULL(charge_priority,5)) DESC, max(user_plan) DESC, min_last_renew_attempt ");

        masterQueryCommand = masterQueryCommandBuilder.toString();

        StringBuilder loadBatchQueryBuilder = new StringBuilder();
        loadBatchQueryBuilder.append("SELECT id, phone, min_last_renew_attempt ");
        loadBatchQueryBuilder.append("FROM ").append(paginationTable);
        loadBatchQueryBuilder.append(" WHERE id > ?");
        loadBatchQueryBuilder.append(" AND id <= ?");

        loadBatchQuery = loadBatchQueryBuilder.toString();
    }
}
