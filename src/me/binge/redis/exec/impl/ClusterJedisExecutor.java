package me.binge.redis.exec.impl;

import java.util.List;

import me.binge.redis.exception.UnSupportMethodException;
import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.utils.RedisCmdPair;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;

/**
 * note: this executor dont support to multi keys at ont time.
 * @author Admin
 *
 */
public class ClusterJedisExecutor extends RedisExecutor<JedisCluster> {

    ClusterJedisExecutor() {
    }

    @Override
    public Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs)
            throws Exception {
        throw new UnSupportMethodException();
    }

    @Override
    public List<Object> multi(List<RedisCmdPair> cmdPairs) throws Exception {
        throw new UnSupportMethodException();
    }

}
