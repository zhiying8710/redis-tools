package me.binge.redis.exec.impl;

import java.lang.reflect.Method;

import me.binge.redis.exec.RedisExecutorProxy;
import me.binge.redis.utils.Close;
import me.binge.redis.utils.DontIntercept;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.JedisCluster;

public class ClusterJedisExecutorProxy extends RedisExecutorProxy<ClusterJedisExecutor, JedisCluster> {

    @Override
    public Object intercept(Object obj, Method method, Object[] args,
            MethodProxy proxy) throws Throwable {

        if (method.getAnnotation(DontIntercept.class) != null) {
            return proxy.invokeSuper(obj, args);
        }

        if (method.getAnnotation(Close.class) != null) {
            return proxy.invokeSuper(obj, new Object[]{jedisCluster});
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
