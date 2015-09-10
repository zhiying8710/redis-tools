package me.binge.redis.exec.impl;

import java.util.List;

import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.utils.EvolutionMethodUtils;
import me.binge.redis.utils.RedisCmdPair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class JedisExecutor extends RedisExecutor<Jedis> {

    JedisExecutor() {
    }

    @Override
    public Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs) throws Exception {

        Pipeline pipeline = this.rtl.conn().pipelined();
        try {
            pipeline.multi();
            for (RedisCmdPair cmdPair : cmdPairs) {
                String cmd = cmdPair.getCmd();

                Object[] oArgs = cmdPair.getoArgs();
                try {
                    EvolutionMethodUtils.invokeMethod(pipeline, cmd, oArgs);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return pipeline.exec();
        } catch (Exception e) {
            if (pipeline != null) {
                pipeline.discard();
            }
            throw e;
        }
    }

    @Override
    public List<Object> multi(List<RedisCmdPair> cmdPairs) throws Exception {

        Transaction trans = this.rtl.conn().multi();
        try {
            for (RedisCmdPair cmdPair : cmdPairs) {
                String cmd = cmdPair.getCmd();

                Object[] oArgs = cmdPair.getoArgs();
                try {
                    EvolutionMethodUtils.invokeMethod(trans, cmd, oArgs);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return trans.exec();
        } catch (Exception e) {
            if (trans != null) {
                trans.discard();
            }
            throw e;
        }
    }
}
