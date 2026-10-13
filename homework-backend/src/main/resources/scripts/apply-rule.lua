-- ARGV[1] = rule version, ARGV[2] = window length in milliseconds.
-- The stored version makes processing the same outbox event idempotent.
local keyType = redis.call('TYPE', KEYS[1]).ok
if keyType == 'none' then
    return 0
end

local now = redis.call('TIME')
local nowMs = now[1] * 1000 + math.floor(now[2] / 1000)

-- Migrate a legacy string counter once, preserving its currently remaining TTL.
if keyType == 'string' then
    local count = redis.call('GET', KEYS[1])
    local ttl = redis.call('PTTL', KEYS[1])
    if ttl <= 0 then
        redis.call('DEL', KEYS[1])
        return 0
    end
    redis.call('DEL', KEYS[1])
    redis.call('HSET', KEYS[1], 'count', count, 'windowStartedAtMs', nowMs - (tonumber(ARGV[2]) - ttl), 'appliedRuleVersion', 0)
    redis.call('PEXPIRE', KEYS[1], ttl)
end

local appliedVersion = tonumber(redis.call('HGET', KEYS[1], 'appliedRuleVersion') or '0')
local requestedVersion = tonumber(ARGV[1])
if appliedVersion >= requestedVersion then
    return redis.call('PTTL', KEYS[1])
end

local startedAtMs = tonumber(redis.call('HGET', KEYS[1], 'windowStartedAtMs'))
local newTtl = tonumber(ARGV[2]) - (nowMs - startedAtMs)
if newTtl <= 0 then
    redis.call('DEL', KEYS[1])
    return 0
end

redis.call('HSET', KEYS[1], 'appliedRuleVersion', requestedVersion)
redis.call('PEXPIRE', KEYS[1], newTtl)
return newTtl
