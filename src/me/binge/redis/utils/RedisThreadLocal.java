package me.binge.redis.utils;


public class RedisThreadLocal<T> {

    private ThreadLocal<T> xtl = new ThreadLocal<T>();

    public T conn() {
        return xtl.get();
    }

    public void conn(T conn) {
        xtl.set(conn);
    }

    public void remove() {
        xtl.remove();
    }


}
