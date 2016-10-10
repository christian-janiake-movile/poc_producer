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
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component("sampleWorker")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SampleWorker implements Worker, Runnable {

    private PeerGroup peerGroup;
    private boolean onDuty = false;
    private long executionStart;

    private Date lastDatabaseQuery;
    private AtomicInteger lastRowRead = new AtomicInteger(0);

    private Integer carrierId;
    private String paginationTable;
    private Integer refreshIntervalMinutes;
    private Integer usersPerMinute;
    private Long forceRefreshIdleTime;

    @Autowired
    RedisConnectionManager redis;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    Logger log;

    @Autowired
    private ApplicationContext ctx;

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
        Validate.isTrue(peerGroup.getProperties().containsKey("engine.usersPerMinute"), "Invalid value for engine.usersPerMinute on peerGroup " + peerGroup.getName());

        this.carrierId = Integer.parseInt(peerGroup.getProperties().getProperty("engine.carrierId"));
        this.paginationTable = peerGroup.getProperties().getProperty("engine.paginationTable");
        this.refreshIntervalMinutes = Integer.parseInt(peerGroup.getProperties().getProperty("engine.refreshIntervalMinutes"));
        this.usersPerMinute = Integer.parseInt(peerGroup.getProperties().getProperty("engine.usersPerMinute"));
    }

    public void work() throws InterruptedException {
        this.onDuty = true;
        synchronized(this) {
            this.notify();
        }
    }

    @PostConstruct
    public void init() {
        securityCache = (SecurityCache) ctx.getBean("securityCache", carrierId);
        loadJdbcStatements();
    }

    public void stop() {
        this.onDuty = false;
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

            while(onDuty) {
                executionStart = System.currentTimeMillis();

                // read & update pagination status on redis
                String redisKey = paginationInfoKey();
                Map<String, String> currentStatus = redis.hgetall(redisKey);

                boolean refreshTable = false;

                Long lastRefresh = null;
                Integer currentOffset = null;
                Integer totalRecords = null;

                if(currentStatus != null && currentStatus.containsKey("lastRefresh")) {
                    lastRefresh = Long.parseLong(currentStatus.get("lastRefresh"));
                    currentOffset = new Integer(currentStatus.getOrDefault("offset", "0"));
                    totalRecords = new Integer(currentStatus.getOrDefault("totalRecords", "0"));

                    if(lastRefresh + TimeUnit.MINUTES.toMillis(refreshIntervalMinutes) < System.currentTimeMillis()) {
                        log.info("Interval exceeded. Generate pagination table {}", paginationTable);
                        refreshTable = true;
                    } else if(!DateUtils.isSameDay(new Date(), new Date(lastRefresh))) {
                        log.info("Turn of the day. Generate pagination table {}", paginationTable);
                        refreshTable = true;
                    } else if(forceRefreshIdleTime != null && forceRefreshIdleTime.longValue() <= System.currentTimeMillis()) {
                        log.info("Producer is idle. Generate pagination table {}", paginationTable);
                        refreshTable = true;
                    }

                } else {
                    log.info("First execution. Generate pagination table {}", paginationTable);
                    refreshTable = true;
                }

                if(refreshTable) {
                    lastRefresh = System.currentTimeMillis();
                    currentOffset = 0;
                    totalRecords = generatePaginationTable();
                }

                if(currentOffset < totalRecords) {

                    List<String> users = fetchUsers();

                    currentOffset += Math.min(totalRecords, currentOffset + usersPerMinute);
                    currentStatus.put("lastRefresh", Long.toString(lastRefresh));
                    currentStatus.put("offset", Integer.toString(currentOffset));
                    currentStatus.put("totalRecords", Integer.toString(totalRecords));
                    redis.hmset(redisKey, currentStatus);

                    Map<String, BloomFilter<CharSequence>> bloomFilters = securityCache.loadBloomFilters();
                    BloomFilter<CharSequence> newBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), usersPerMinute);

                    // check & update cache & send to queue
                    for(String msisdn:users) {

                        if(!securityCache.isOnCache(msisdn, bloomFilters)) {
                            newBloomFilter.put(msisdn);
                            securityCache.putOnSecurityCache(msisdn, executionStart);
                        }

                    }
                    securityCache.writeBloomFilter(newBloomFilter, executionStart);

                } else if(forceRefreshIdleTime == null) {
                    forceRefreshIdleTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
                }
            }
        }
    }

    private Integer generatePaginationTable() {

        return jdbcTemplate.update(masterQueryCommand);
    }

    private List<String> fetchUsers() {
        List<String> result = jdbcTemplate.query(loadBatchQuery, new Object[] {offset, (offset + usersPerMinute)}, new RowMapper() {

            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                offset = rs.getInt("id");
                String phone = rs.getString("phone");
                log.info("User {} loaded, offset={}", phone, offset);
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
