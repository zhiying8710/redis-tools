package me.binge.redis.exec.impl;

import java.util.List;

import me.binge.redis.exception.UnSupportMethodException;
import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.utils.RedisCmdPair;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;

public class ShardedJedisExecutor extends RedisExecutor<ShardedJedis> {

    ShardedJedisExecutor() {
    }

    @Override
    public Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs) throws UnSupportMethodException {
        throw new UnSupportMethodException();
    }

    @Override
    public List<Object> multi(List<RedisCmdPair> cmdPairs) throws UnSupportMethodException {
        throw new UnSupportMethodException();
    }

}
