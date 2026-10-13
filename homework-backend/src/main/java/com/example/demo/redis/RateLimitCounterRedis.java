package com.example.demo.redis;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

/**
 * Centralizes all Redis access used by rate limiting.
 *
 * <p>The Lua script keeps incrementing a counter and assigning its initial
 * expiration in one atomic Redis operation, preventing concurrent requests
 * from accidentally creating a counter without a TTL.</p>
 */
@Component
public class RateLimitCounterRedis {

    private static final String KEY_PREFIX = "rate-limit:";
    private static final DefaultRedisScript<Long> INCREMENT_WITH_EXPIRY = incrementWithExpiryScript();
    private static final DefaultRedisScript<Long> APPLY_RULE = applyRuleScript();

    private final StringRedisTemplate redisTemplate;

    public RateLimitCounterRedis(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public long increment(String apiKey, long windowSeconds, long ruleVersion) {
        Long usage = redisTemplate.execute(
                INCREMENT_WITH_EXPIRY,
                List.of(keyFor(apiKey)),
                Long.toString(Math.multiplyExact(windowSeconds, 1_000)),
                Long.toString(ruleVersion));
        if (usage == null) {
            throw new IllegalStateException("Redis did not return a usage counter");
        }
        return usage;
    }

    public long currentUsage(String apiKey) {
        String key = keyFor(apiKey);
        if (redisTemplate.type(key) == DataType.STRING) {
            String storedUsage = redisTemplate.opsForValue().get(key);
            return storedUsage == null ? 0 : Long.parseLong(storedUsage);
        }
        Object storedUsage = redisTemplate.opsForHash().get(key, "count");
        return storedUsage == null ? 0 : Long.parseLong(storedUsage.toString());
    }

    public long ttlSeconds(String apiKey) {
        Long ttl = redisTemplate.getExpire(keyFor(apiKey), TimeUnit.SECONDS);
        return ttl == null || ttl < 0 ? 0 : ttl;
    }

    public void clear(String apiKey) {
        redisTemplate.delete(keyFor(apiKey));
    }

    /**
     * Keeps the existing usage counter but changes its expiry as if the new
     * window duration had been in effect since the counter was first created.
     *
     * <p>The calculation and expiry update run inside Redis as one atomic
     * operation, so a concurrent increment cannot be lost between reading the
     * old TTL and applying the new one.</p>
     */
    public void applyRule(String apiKey, long ruleVersion, long windowSeconds) {
        redisTemplate.execute(
                APPLY_RULE,
                List.of(keyFor(apiKey)),
                Long.toString(ruleVersion),
                Long.toString(Math.multiplyExact(windowSeconds, 1_000)));
    }

    private static DefaultRedisScript<Long> incrementWithExpiryScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("scripts/increment-with-expiry.lua")));
        script.setResultType(Long.class);
        return script;
    }

    private static DefaultRedisScript<Long> applyRuleScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("scripts/apply-rule.lua")));
        script.setResultType(Long.class);
        return script;
    }

    private String keyFor(String apiKey) {
        return KEY_PREFIX + apiKey;
    }
}
