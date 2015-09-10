package me.binge.redis.exec.impl;

import java.util.List;

import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.utils.RedisCmdPair;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;

public class ShardedJedisExecutor extends RedisExecutor<ShardedJedis> {

    ShardedJedisExecutor() {
    }

    @Override
    public Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs) throws Exception {
        throw new UnsupportedOperationException("sharding jedis cluster can not exec pipeline cmd.");
    }

    @Override
    public List<Object> multi(List<RedisCmdPair> cmdPairs) throws Exception {
        throw new UnsupportedOperationException("sharding jedis can not exec multi cmd.");
    }

}
