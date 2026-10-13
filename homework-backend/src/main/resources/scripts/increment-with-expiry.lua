-- ARGV[1] = window length in milliseconds, ARGV[2] = current rule version.
local keyType = redis.call('TYPE', KEYS[1]).ok
local now = redis.call('TIME')
local nowMs = now[1] * 1000 + math.floor(now[2] / 1000)

-- Migrate the old string format without losing its count or remaining TTL.
if keyType == 'string' then
    local count = redis.call('GET', KEYS[1])
    local ttl = redis.call('PTTL', KEYS[1])
    redis.call('DEL', KEYS[1])
    if ttl > 0 then
        redis.call('HSET', KEYS[1], 'count', count, 'windowStartedAtMs', nowMs - (tonumber(ARGV[1]) - ttl), 'appliedRuleVersion', 0)
        redis.call('PEXPIRE', KEYS[1], ttl)
        keyType = 'hash'
    else
        keyType = 'none'
    end
end

if keyType == 'hash' then
    local appliedVersion = tonumber(redis.call('HGET', KEYS[1], 'appliedRuleVersion') or '0')
    local requestedVersion = tonumber(ARGV[2])
    if appliedVersion < requestedVersion then
        local startedAtMs = tonumber(redis.call('HGET', KEYS[1], 'windowStartedAtMs'))
        local newTtl = tonumber(ARGV[1]) - (nowMs - startedAtMs)
        if newTtl <= 0 then
            redis.call('DEL', KEYS[1])
            keyType = 'none'
        else
            redis.call('HSET', KEYS[1], 'appliedRuleVersion', requestedVersion)
            redis.call('PEXPIRE', KEYS[1], newTtl)
        end
    end
end

if keyType == 'none' then
    redis.call('HSET', KEYS[1], 'count', 1, 'windowStartedAtMs', nowMs, 'appliedRuleVersion', ARGV[2])
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
    return 1
end

return redis.call('HINCRBY', KEYS[1], 'count', 1)
