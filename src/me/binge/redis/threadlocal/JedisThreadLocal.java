package me.binge.redis.threadlocal;

import redis.clients.jedis.Jedis;

public class JedisThreadLocal {
    private static ThreadLocal<Jedis> jedisTl = new ThreadLocal<Jedis>();

    public static Jedis get() {
        return jedisTl.get();
    }

    public static void set(Jedis j) {
        jedisTl.set(j);
    }

    public static void remove() {
        jedisTl.remove();
    }
}
