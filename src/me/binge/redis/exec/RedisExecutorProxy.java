package me.binge.redis.exec;

import java.io.Closeable;
import java.lang.reflect.Method;

import me.binge.redis.utils.Close;
import me.binge.redis.utils.DontIntercept;
import me.binge.redis.utils.RedisThreadLocal;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.util.Pool;

public abstract class RedisExecutorProxy<E extends RedisExecutor<T>, T extends JedisCommands>
        implements MethodInterceptor {

    protected static final Class<Void> VOID = Void.TYPE;

    private RedisExecutor<T> executor;

    protected RedisThreadLocal<T> rtl;

    protected Pool<T> pool;

    protected JedisCluster jedisCluster;

    @SuppressWarnings("unchecked")
    public RedisExecutor<T> getExecutor() {
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

    @Override
    public Object intercept(Object obj, Method method, Object[] args,
            MethodProxy proxy) throws Throwable {
        if (method.getAnnotation(DontIntercept.class) != null) {
            return proxy.invokeSuper(obj, args);
        }

        if (method.getAnnotation(Close.class) != null) {
            Method closeMethod = null;
            try {
                closeMethod = obj.getClass().getDeclaredMethod(method.getName(), Closeable.class);
            } catch (Exception e) {
            }
            if (closeMethod == null) {
                return null;
            }
            closeMethod.setAccessible(true);
            return closeMethod.invoke(obj, this.getCloseable());
        }
        return VOID;
    }

    public void release(T conn, boolean broken) {
        if (conn == null) {
            return;
        }
        if (broken) {
            this.pool.returnBrokenResource(conn);
        } else {
            this.pool.returnResource(conn);
        }
    }

    public abstract Class<E> getProxiedClass();

    public Closeable getCloseable() {
        if (this.pool != null) {
            return this.pool;
        }
        if (this.jedisCluster != null) {
            return this.jedisCluster;
        }
        return null;
    }
}
