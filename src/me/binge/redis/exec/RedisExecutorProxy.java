package me.binge.redis.exec;

import me.binge.redis.exec.impl.RedisThreadLocal;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.util.Pool;

public abstract class RedisExecutorProxy<E extends RedisExecutor<T>, T extends JedisCommands> implements MethodInterceptor {

    private RedisExecutor<T> executor;

    protected RedisThreadLocal<T> rtl;

    protected Pool<T> pool;

    protected JedisCluster jedisCluster;

    @SuppressWarnings("unchecked")
    public RedisExecutor<T> getExecutor(){
        if (executor == null) {
            synchronized (RedisExecutorProxy.class) {
                if (executor == null) {
                    this.rtl = new RedisThreadLocal<T>();
                    Enhancer enhancer = new Enhancer();
                    enhancer.setSuperclass(getProxiedClass());
                    enhancer.setCallback(this);
                    executor = (RedisExecutor<T>) enhancer.create();
                    executor.setThreadLocal(this.rtl);
                }
            }
        }
        return executor;
    }

    public void release(T conn, boolean broken) {
        if (conn == null) {
            return;
        }
        if (broken) {
            getPool().returnBrokenResource(conn);
        } else {
            getPool().returnResource(conn);
        }
    }

    public Pool<T> getPool(){
        return this.pool;
    }

    public abstract Class<E> getProxiedClass();

    public void setPool(Pool<T> pool) {
        this.pool = pool;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                if (RedisExecutorProxy.this.pool != null) {
                    RedisExecutorProxy.this.pool.close();
                }
            }
        }));
    }

    public void setCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                if (RedisExecutorProxy.this.jedisCluster != null) {
                    RedisExecutorProxy.this.jedisCluster.close();
                }
            }
        }));
    }

    public JedisCluster getCluster() {
        return this.jedisCluster;
    }
}
