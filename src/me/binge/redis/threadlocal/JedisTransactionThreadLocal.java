package me.binge.redis.threadlocal;

import redis.clients.jedis.Transaction;

public class JedisTransactionThreadLocal {

    private static ThreadLocal<Transaction> jedisTransTl = new ThreadLocal<Transaction>();

    public static Transaction get() {
        return jedisTransTl.get();
    }

    public static void set(Transaction t) {
        jedisTransTl.set(t);
    }

    public static void remove() {
        jedisTransTl.remove();
    }


}
