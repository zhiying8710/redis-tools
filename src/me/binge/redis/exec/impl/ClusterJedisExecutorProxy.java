package me.binge.redis.exec.impl;

import java.lang.reflect.Method;

import me.binge.redis.exec.RedisExecutorProxy;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.JedisCluster;

public class ClusterJedisExecutorProxy extends RedisExecutorProxy<ClusterJedisExecutor, JedisCluster> {

    @Override
    public Object intercept(Object obj, Method method, Object[] args,
            MethodProxy proxy) throws Throwable {

        Object r = super.intercept(obj, method, args, proxy);
        if (VOID != r) {
            return r;
        }

        this.rtl.conn(jedisCluster);
        try {
            return proxy.invokeSuper(obj, args);
        } finally {
            this.rtl.remove();
        }

    }

    @Override
    public Class<ClusterJedisExecutor> getProxiedClass() {
        return ClusterJedisExecutor.class;
    }


}
