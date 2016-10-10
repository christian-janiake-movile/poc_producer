package com.movile.pocproducer;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.lambdaworks.redis.ScoredValue;
import com.movile.res.redis.RedisConnectionManager;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christian.janiake on 06/10/16.
 *
 * Subscription Engine Security Cache is used to control user processing submission and avoid duplicated billing on
 * renewal process. Every result set obtained from a query and filtered on the cache is stored as a bloomfilter, on
 * a sorted set that indicates the time when the batch was submitted.
 * Expiration control is done when the set is read.
 * Besides the bloomfilter list, every batch is also stored as a sorted set, identified with the timestamp as part of
 * the key and with the phone numbers as the members.
 *
 * sbs.engine.<carrierId>.bloomfilterset: sorted set with timestamp as score, and bloomfilters as members
 * sbs.engine.<carrierId>.<batchIdentifier>: sorted set with phone number as score and member; the timestamp is used
 *                                           as the batch identifier
 */
@Component("securityCache")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SecurityCache {
    private Integer carrierId;

    @Autowired
    public SecurityCache(@Value("#{carrierId}") Integer carrierId) {
        this.carrierId = carrierId;
    }

    @Value("#{systemProperties['redis.sbs.engine.key.prefix'] ?: 'sbs.engine'}")
    private String keyPrefix;
    private String redisPrefix;

    private final static String BLOOMFILTER_SET_SUFFIX = "bloomfilterset";

    @Autowired
    RedisConnectionManager redis;

    @Autowired
    Logger log;

    @PostConstruct
    public void init() {
        redisPrefix = keyPrefix + "." + carrierId + ".";
    }

    public boolean isOnCache(String msisdn, Map<String, BloomFilter<CharSequence>> bloomFilters) {
        for(Map.Entry<String, BloomFilter<CharSequence>> mapentry:bloomFilters.entrySet()) {
            String batchIdentifier = mapentry.getKey();
            BloomFilter<CharSequence> bf = mapentry.getValue();
            if(bf.mightContain(msisdn) && isUserOnBatch(batchIdentifier, msisdn)) {
                return true;
            }
        }
        return false;
    }

    public boolean isUserOnBatch(String batchIdentifier, String msisdn) {
        double numericMsisdn = Double.parseDouble(msisdn);
        List<String> searchResult = redis.zrangebyscore(redisPrefix + batchIdentifier, numericMsisdn, numericMsisdn);
        return searchResult != null && !searchResult.isEmpty();
    }

    public void putOnSecurityCache(String msisdn, Long batchIdentifier) {
        String key = redisPrefix + batchIdentifier.toString();
        redis.zadd(key, Double.valueOf(msisdn), msisdn);
    }

    public Map<String, BloomFilter<CharSequence>> loadBloomFilters() {

        Long now = new Long(System.currentTimeMillis());
        Long purgeExpiredRecords = redis.zremrangebyscore(redisPrefix + BLOOMFILTER_SET_SUFFIX, 0, now.doubleValue());
        log.info("Removing {} expired records", purgeExpiredRecords);

        List<ScoredValue<String>> searchResult = redis.zrangebyscoreWithScores(redisPrefix + BLOOMFILTER_SET_SUFFIX, 0, Double.MAX_VALUE);
        Map<String, BloomFilter<CharSequence>> resultMap = new HashMap<String, BloomFilter<CharSequence>>();

        for(ScoredValue<String> scoredValue:searchResult) {
            try {
                String batchIdentifier = Double.toString(scoredValue.score);
                BloomFilter<CharSequence> bloomfilter = BloomFilter
                        .readFrom(new ByteArrayInputStream(scoredValue.value.getBytes(StandardCharsets.UTF_8)), Funnels.stringFunnel(Charset.forName("UTF-8")));
                resultMap.put(batchIdentifier, bloomfilter);

            } catch(Exception e) {
                log.error("Error reading bloomfilters", e);
            }

        }
        return resultMap;
    }

    public void writeBloomFilter(BloomFilter<CharSequence> newBloomFilter, Long batchIdentifier) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            newBloomFilter.writeTo(baos);
            Long result = redis.zadd(redisPrefix + BLOOMFILTER_SET_SUFFIX, batchIdentifier.doubleValue(), baos.toString());

            log.info("Bloomfilter stored with score {} ({})", batchIdentifier, result);

        } catch(Exception e) {
            log.error("Error writing bloomfilters", e);
        }
    }
}
