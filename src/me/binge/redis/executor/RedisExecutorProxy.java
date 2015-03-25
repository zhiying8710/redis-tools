package me.binge.redis.executor;

import java.lang.reflect.Method;

import me.binge.redis.annotation.Pipe;
import me.binge.redis.annotation.Trans;
import me.binge.redis.exception.RedisExecExecption;
import me.binge.redis.threadlocal.JedisPipelineThreadLocal;
import me.binge.redis.threadlocal.JedisThreadLocal;
import me.binge.redis.threadlocal.JedisTransactionThreadLocal;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class RedisExecutorProxy implements MethodInterceptor {

    private Pool<Jedis> jedisPool;

    private RedisExecutor executor;

    public RedisExecutor getInstance() {
        if (executor == null) {
            synchronized (RedisExecutorProxy.class) {
                if (executor == null) {
                    Enhancer enhancer = new Enhancer();
                    enhancer.setSuperclass(RedisExecutor.class);
                    enhancer.setCallback(this);
                    executor = (RedisExecutor) enhancer.create();
                }
            }
        }
        return executor;

    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args,
            MethodProxy proxy) throws Throwable {

        Jedis jedis = get();
        JedisThreadLocal.set(jedis);
        Trans trans = method.getAnnotation(Trans.class);
        if (trans != null) {
            JedisTransactionThreadLocal.set(jedis.multi());
        }
        Pipe pipe = method.getAnnotation(Pipe.class);
        if (pipe != null) {
            System.err.println(pipe);
            JedisPipelineThreadLocal.set(jedis.pipelined());
        }
        boolean broken = false;
        try {
            return proxy.invokeSuper(obj, args);
        } catch (Exception e) {
            if (trans != null) {
                JedisTransactionThreadLocal.get().discard();
            }
            if (pipe != null) {
                JedisPipelineThreadLocal.get().discard();
            }
            broken = true;
            throw new RedisExecExecption("exec " + method + " with " + StringUtils.join(args, ',') + " error.", e);
        } finally {
            JedisTransactionThreadLocal.remove();
            JedisPipelineThreadLocal.remove();
            JedisThreadLocal.remove();
            release(jedis, broken);
        }

    }

    private void release(Jedis jedis, boolean isBroken) {
        if (isBroken) {
            this.jedisPool.returnBrokenResource(jedis);
        } else {
            this.jedisPool.returnResource(jedis);
        }
    }

    private Jedis get() {
        return jedisPool.getResource();
    }

    public void setJedisPool(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

//    public static void main(String[] args) throws Exception {
//
//
//        RedisExecutorProxy redisExecutorProxy = new RedisExecutorProxy();
//        JedisPool jedisPool = new JedisPool("127.0.0.1");
//        redisExecutorProxy.setJedisPool(jedisPool);
//
//        redisExecutorProxy.getInstance().set("111", "222");
//
//        String a = redisExecutorProxy.getInstance().get("111");
//        System.out.println(a);
//
//        RedisCmdPair cmdPair = new RedisCmdPair();
//        cmdPair.setCmd("zadd");
//        cmdPair.setoArgs(new Object[]{"oooo", new Double(101), "kkkkk"});
//        redisExecutorProxy.getInstance().cmd(cmdPair);
//
//        List<RedisCmdPair> cmdPairs = new ArrayList<RedisCmdPair>();
//        RedisCmdPair cmdPair1 = new RedisCmdPair();
//        cmdPair1.setCmd("hset");
//        cmdPair1.setoArgs(new Object[]{"yyyy", "zzzz", "kkkkk"});
//        cmdPairs.add(cmdPair1);
//        redisExecutorProxy.getInstance().multi(cmdPairs);
//
//    }

}
