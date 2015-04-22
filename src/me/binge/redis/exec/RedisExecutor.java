package me.binge.redis.exec;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.binge.redis.exception.RedisExecExecption;
import me.binge.redis.exec.impl.RedisThreadLocal;
import me.binge.redis.utils.DontIntercept;
import me.binge.redis.utils.EvolutionMethodUtils;
import me.binge.redis.utils.RedisCmdPair;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

public abstract class RedisExecutor<T extends JedisCommands> implements JedisCommands {

    protected RedisThreadLocal<T> rtl;

    public abstract Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs) throws Exception;

    public abstract List<Object> multi(List<RedisCmdPair> cmdPairs) throws Exception;

    @SuppressWarnings("unchecked")
    public <E> E cmd(RedisCmdPair cmdPair) {
        try {
            return (E) EvolutionMethodUtils.invokeMethod(this.rtl.conn(), cmdPair.getCmd(), cmdPair.getoArgs());
        } catch (Exception e) {
            throw new RedisExecExecption(cmdPair.toString(), e);
        }
    }

    @DontIntercept
    void setThreadLocal(RedisThreadLocal<T> rtl) {
        this.rtl = rtl;
    }

    @Override
    public String set(String key, String value) {
        return this.cmd(new RedisCmdPair("set", new Object[]{key, value}));
    }

    @Override
    public String set(String key, String value, String nxxx, String expx,
            long time) {
        return this.cmd(new RedisCmdPair("set", new Object[]{key, value, nxxx, expx, time}));
    }

    @Override
    public String get(String key) {
        return this.cmd(new RedisCmdPair("get", new Object[]{key}));
    }

    @Override
    public Boolean exists(String key) {
        return this.cmd(new RedisCmdPair("exists", new Object[]{key}));
    }

    @Override
    public Long persist(String key) {
        return this.cmd(new RedisCmdPair("persist", new Object[]{key}));
    }

    @Override
    public String type(String key) {
        return this.cmd(new RedisCmdPair("type", new Object[]{key}));
    }

    @Override
    public Long expire(String key, int seconds) {
        return this.cmd(new RedisCmdPair("expire", new Object[]{key, seconds}));
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return this.cmd(new RedisCmdPair("expireAt", new Object[]{key, unixTime}));
    }

    @Override
    public Long ttl(String key) {
        return this.cmd(new RedisCmdPair("ttl", new Object[]{key}));
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return this.cmd(new RedisCmdPair("setbit", new Object[]{key, offset, value}));
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return this.cmd(new RedisCmdPair("setbit", new Object[]{key, offset, value}));
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return this.cmd(new RedisCmdPair("getbit", new Object[]{key, offset}));
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return this.cmd(new RedisCmdPair("setrange", new Object[]{key, offset, value}));
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return this.cmd(new RedisCmdPair("getrange", new Object[]{key, startOffset, endOffset}));
    }

    @Override
    public String getSet(String key, String value) {
        return this.cmd(new RedisCmdPair("getSet", new Object[]{key, value}));
    }

    @Override
    public Long setnx(String key, String value) {
        return this.cmd(new RedisCmdPair("setnx", new Object[]{key, value}));
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return this.cmd(new RedisCmdPair("setex", new Object[]{key, seconds, value}));
    }

    @Override
    public Long decrBy(String key, long integer) {
        return this.cmd(new RedisCmdPair("decrBy", new Object[]{key, integer}));
    }

    @Override
    public Long decr(String key) {
        return this.cmd(new RedisCmdPair("decr", new Object[]{key}));
    }

    @Override
    public Long incrBy(String key, long integer) {
        return this.cmd(new RedisCmdPair("incrBy", new Object[]{key, integer}));
    }

    @Override
    public Long incr(String key) {
        return this.cmd(new RedisCmdPair("incr", new Object[]{key}));
    }

    @Override
    public Long append(String key, String value) {
        return this.cmd(new RedisCmdPair("append", new Object[]{key, value}));
    }

    @Override
    public String substr(String key, int start, int end) {
        return this.cmd(new RedisCmdPair("substr", new Object[]{key, start, end}));
    }

    @Override
    public Long hset(String key, String field, String value) {
        return this.cmd(new RedisCmdPair("hset", new Object[]{key, field, value}));
    }

    @Override
    public String hget(String key, String field) {
        return this.cmd(new RedisCmdPair("hget", new Object[]{key, field}));
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return this.cmd(new RedisCmdPair("hsetnx", new Object[]{key, field, value}));
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return this.cmd(new RedisCmdPair("hmset", new Object[]{key, hash}));
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return this.cmd(new RedisCmdPair("hmget", new Object[]{key, fields}));
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return this.cmd(new RedisCmdPair("hincrBy", new Object[]{key, field, value}));
    }

    @Override
    public Boolean hexists(String key, String field) {
        return this.cmd(new RedisCmdPair("hexists", new Object[]{key, field}));
    }

    @Override
    public Long hdel(String key, String... fields) {
        return this.cmd(new RedisCmdPair("hdel", new Object[]{key, fields}));
    }

    @Override
    public Long hlen(String key) {
        return this.cmd(new RedisCmdPair("hlen", new Object[]{key}));
    }

    @Override
    public Set<String> hkeys(String key) {
        return this.cmd(new RedisCmdPair("hkeys", new Object[]{key}));
    }

    @Override
    public List<String> hvals(String key) {
        return this.cmd(new RedisCmdPair("hvals", new Object[]{key}));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return this.cmd(new RedisCmdPair("hgetAll", new Object[]{key}));
    }

    @Override
    public Long rpush(String key, String... string) {
        return this.cmd(new RedisCmdPair("rpush", new Object[]{key, string}));
    }

    @Override
    public Long lpush(String key, String... string) {
        return this.cmd(new RedisCmdPair("lpush", new Object[]{key, string}));
    }

    @Override
    public Long llen(String key) {
        return this.cmd(new RedisCmdPair("llen", new Object[]{key}));
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("lrange", new Object[]{key, start, end}));
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("ltrim", new Object[]{key, start, end}));
    }

    @Override
    public String lindex(String key, long index) {
        return this.cmd(new RedisCmdPair("lindex", new Object[]{key, index}));
    }

    @Override
    public String lset(String key, long index, String value) {
        return this.cmd(new RedisCmdPair("lset", new Object[]{key, index, value}));
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return this.cmd(new RedisCmdPair("lrem", new Object[]{key, count, value}));
    }

    @Override
    public String lpop(String key) {
        return this.cmd(new RedisCmdPair("lpop", new Object[]{key}));
    }

    @Override
    public String rpop(String key) {
        return this.cmd(new RedisCmdPair("rpop", new Object[]{key}));
    }

    @Override
    public Long sadd(String key, String... member) {
        return this.cmd(new RedisCmdPair("sadd", new Object[]{key, member}));
    }

    @Override
    public Set<String> smembers(String key) {
        return this.cmd(new RedisCmdPair("smembers", new Object[]{key}));
    }

    @Override
    public Long srem(String key, String... member) {
        return this.cmd(new RedisCmdPair("srem", new Object[]{key, member}));
    }

    @Override
    public String spop(String key) {
        return this.cmd(new RedisCmdPair("spop", new Object[]{key}));
    }

    @Override
    public Long scard(String key) {
        return this.cmd(new RedisCmdPair("scard", new Object[]{key}));
    }

    @Override
    public Boolean sismember(String key, String member) {
        return this.cmd(new RedisCmdPair("sismember", new Object[]{key, member}));
    }

    @Override
    public String srandmember(String key) {
        return this.cmd(new RedisCmdPair("srandmember", new Object[]{key}));
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return this.cmd(new RedisCmdPair("srandmember", new Object[]{key, count}));
    }

    @Override
    public Long strlen(String key) {
        return this.cmd(new RedisCmdPair("strlen", new Object[]{key}));
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return this.cmd(new RedisCmdPair("zadd", new Object[]{key, score, member}));
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return this.cmd(new RedisCmdPair("zadd", new Object[]{key, scoreMembers}));
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("zrange", new Object[]{key, start, end}));
    }

    @Override
    public Long zrem(String key, String... member) {
        return this.cmd(new RedisCmdPair("zrem", new Object[]{key, member}));
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return this.cmd(new RedisCmdPair("zincrby", new Object[]{key, score, member}));
    }

    @Override
    public Long zrank(String key, String member) {
        return this.cmd(new RedisCmdPair("zrank", new Object[]{key, member}));
    }

    @Override
    public Long zrevrank(String key, String member) {
        return this.cmd(new RedisCmdPair("zrevrank", new Object[]{key, member}));
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("zrevrange", new Object[]{key, start, end}));
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("zrangeWithScores", new Object[]{key, start, end}));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("zrevrangeWithScores", new Object[]{key, start, end}));
    }

    @Override
    public Long zcard(String key) {
        return this.cmd(new RedisCmdPair("zcard", new Object[]{key}));
    }

    @Override
    public Double zscore(String key, String member) {
        return this.cmd(new RedisCmdPair("zscore", new Object[]{key, member}));
    }

    @Override
    public List<String> sort(String key) {
        return this.cmd(new RedisCmdPair("sort", new Object[]{key}));
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return this.cmd(new RedisCmdPair("sort", new Object[]{key, sortingParameters}));
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return this.cmd(new RedisCmdPair("zcount", new Object[]{key, min, max}));
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zcount", new Object[]{key, min, max}));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return this.cmd(new RedisCmdPair("zrangeByScore", new Object[]{key, min, max}));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zrangeByScore", new Object[]{key, min, max}));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return this.cmd(new RedisCmdPair("zrevrangeByScore", new Object[]{key, max, min}));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrangeByScore", new Object[]{key, min, max, offset, count}));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return this.cmd(new RedisCmdPair("zrevrangeByScore", new Object[]{key, max, min}));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrangeByScore", new Object[]{key, min, max, offset, count}));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrevrangeByScore", new Object[]{key, max, min, offset, count}));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return this.cmd(new RedisCmdPair("zrangeByScoreWithScores", new Object[]{key, min, max}));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
            double min) {
        return this.cmd(new RedisCmdPair("zrevrangeByScoreWithScores", new Object[]{key, max, min}));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min,
            double max, int offset, int count) {
        return this.cmd(new RedisCmdPair("zrangeByScoreWithScores", new Object[]{key, min, max, offset, count}));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrevrangeByScore", new Object[]{key, max, min, offset, count}));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zrangeByScoreWithScores", new Object[]{key, min, max}));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
            String min) {
        return this.cmd(new RedisCmdPair("zrevrangeByScoreWithScores", new Object[]{key, max, min}));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min,
            String max, int offset, int count) {
        return this.cmd(new RedisCmdPair("zrangeByScoreWithScores", new Object[]{key, min, max, offset, count}));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
            double min, int offset, int count) {
        return this.cmd(new RedisCmdPair("zrevrangeByScoreWithScores", new Object[]{key, max, min, offset, count}));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
            String min, int offset, int count) {
        return this.cmd(new RedisCmdPair("zrevrangeByScoreWithScores", new Object[]{key, max, min, offset, count}));
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("zremrangeByRank", new Object[]{key, start, end}));
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return this.cmd(new RedisCmdPair("zremrangeByScore", new Object[]{key, start, end}));
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return this.cmd(new RedisCmdPair("zremrangeByScore", new Object[]{key, start, end}));
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zlexcount", new Object[]{key, min, max}));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zrangeByLex", new Object[]{key, min, max}));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrangeByLex", new Object[]{key, min, max, offset, count}));
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return this.cmd(new RedisCmdPair("zremrangeByLex", new Object[]{key, min, max}));
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot,
            String value) {
        return this.cmd(new RedisCmdPair("linsert", new Object[]{key, where, pivot, value}));
    }

    @Override
    public Long lpushx(String key, String... string) {
        return this.cmd(new RedisCmdPair("lpushx", new Object[]{key, string}));
    }

    @Override
    public Long rpushx(String key, String... string) {
        return this.cmd(new RedisCmdPair("rpushx", new Object[]{key, string}));
    }

    @Override
    public List<String> blpop(String arg) {
        return this.cmd(new RedisCmdPair("blpop", new Object[]{arg}));
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return this.cmd(new RedisCmdPair("blpop", new Object[]{timeout, key}));
    }

    @Override
    public List<String> brpop(String arg) {
        return this.cmd(new RedisCmdPair("brpop", new Object[]{arg}));
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return this.cmd(new RedisCmdPair("brpop", new Object[]{timeout, key}));
    }

    @Override
    public Long del(String key) {
        return this.cmd(new RedisCmdPair("del", new Object[]{key}));
    }

    @Override
    public String echo(String string) {
        return this.cmd(new RedisCmdPair("echo", new Object[]{string}));
    }

    @Override
    public Long move(String key, int dbIndex) {
        return this.cmd(new RedisCmdPair("move", new Object[]{key, dbIndex}));
    }

    @Override
    public Long bitcount(String key) {
        return this.cmd(new RedisCmdPair("bitcount", new Object[]{key}));
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return this.cmd(new RedisCmdPair("bitcount", new Object[]{key, start, end}));
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
        return this.cmd(new RedisCmdPair("hscan", new Object[]{key, cursor}));
    }

    @Override
    public ScanResult<String> sscan(String key, int cursor) {
        return this.cmd(new RedisCmdPair("sscan", new Object[]{key, cursor}));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, int cursor) {
        return this.cmd(new RedisCmdPair("zscan", new Object[]{key, cursor}));
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
        return this.cmd(new RedisCmdPair("hscan", new Object[]{key, cursor}));
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return this.cmd(new RedisCmdPair("sscan", new Object[]{key, cursor}));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return this.cmd(new RedisCmdPair("zscan", new Object[]{key, cursor}));
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return this.cmd(new RedisCmdPair("pfadd", new Object[]{key, elements}));
    }

    @Override
    public long pfcount(String key) {
        return this.cmd(new RedisCmdPair("pfcount", new Object[]{key}));
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return this.cmd(new RedisCmdPair("pexpire", new Object[]{key, milliseconds}));
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return this.cmd(new RedisCmdPair("pexpireAt", new Object[]{key, millisecondsTimestamp}));
    }

    @Override
    public Double incrByFloat(String key, double value) {
        return this.cmd(new RedisCmdPair("incrByFloat", new Object[]{key, value}));
    }

    @Override
    public Set<String> spop(String key, long count) {
        return this.cmd(new RedisCmdPair("spop", new Object[]{key, count}));
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return this.cmd(new RedisCmdPair("zrevrangeByLex", new Object[]{key, max, min}));
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min,
            int offset, int count) {
        return this.cmd(new RedisCmdPair("zrevrangeByLex", new Object[]{key, max, min, offset, count}));
    }
}
