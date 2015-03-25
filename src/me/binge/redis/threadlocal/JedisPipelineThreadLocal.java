package me.binge.redis.threadlocal;

import redis.clients.jedis.Pipeline;

public class JedisPipelineThreadLocal {

    private static ThreadLocal<Pipeline> jedisPipeTl = new ThreadLocal<Pipeline>();

    public static Pipeline get() {
        return jedisPipeTl.get();
    }

    public static void set(Pipeline p) {
        jedisPipeTl.set(p);
    }

    public static void remove() {
        jedisPipeTl.remove();
    }


}
