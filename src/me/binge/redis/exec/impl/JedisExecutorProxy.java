package me.binge.redis.exec.impl;

import me.binge.redis.exception.RedisExecExecption;
import me.binge.redis.exec.RedisExecutorProxy;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class JedisExecutorProxy extends RedisExecutorProxy<JedisExecutor, Jedis> {

    public JedisExecutorProxy(Pool<Jedis> pool) {
        if (pool == null) {
            throw new NullPointerException("pool can not be null.");
        }
        this.pool = pool;
    }

    @Override
    public Object intercept(Object obj, java.lang.reflect.Method method,
            Object[] args, MethodProxy proxy) throws Throwable {

        Object r = super.intercept(obj, method, args, proxy);
        if (VOID != r) {
            return r;
        }
        Jedis jedis = pool.getResource();
        this.rtl.conn(jedis);
        boolean broken = false;
        try {
            return proxy.invokeSuper(obj, args);
        } catch (Exception e) {
            broken = true;
            throw new RedisExecExecption("exec " + method + " with "
                    + StringUtils.join(args, ',') + " error.", e);
        } finally {
            this.rtl.remove();
            release(jedis, broken);
        }
    }

    @Override
    public Class<JedisExecutor> getProxiedClass() {
        return JedisExecutor.class;
    }

}
