package me.binge.redis.executor;

import java.util.List;

import me.binge.redis.RedisCmdPair;
import me.binge.redis.annotation.Pipe;
import me.binge.redis.annotation.Trans;
import me.binge.redis.threadlocal.JedisPipelineThreadLocal;
import me.binge.redis.threadlocal.JedisThreadLocal;
import me.binge.redis.threadlocal.JedisTransactionThreadLocal;
import me.binge.redis.utils.EvolutionMethodUtils;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;


class RedisExecutor {


    public void set(String key, String value) {
        JedisThreadLocal.get().set(key, value);
    }

    public String get(String key) {
        return JedisThreadLocal.get().get(key);
    }

    @Pipe
    public Response<List<Object>> pipeline(List<RedisCmdPair> cmdPairs) {

        Pipeline pipeline = JedisPipelineThreadLocal.get();
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
    }

    @Trans
    public List<Object> multi(List<RedisCmdPair> cmdPairs) {

        Transaction trans = JedisTransactionThreadLocal.get();
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
    }

    @SuppressWarnings("unchecked")
    public <T> T cmd(RedisCmdPair cmdPair) throws Exception {
        return (T) EvolutionMethodUtils.invokeMethod(JedisThreadLocal.get(), cmdPair.getCmd(), cmdPair.getoArgs());
    }

}
