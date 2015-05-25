package me.binge.redis.test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.exec.impl.ClusterJedisExecutorProxy;
import me.binge.redis.exec.impl.JedisExecutorProxy;
import me.binge.redis.exec.impl.SentinelJedisExecutorProxy;
import me.binge.redis.exec.impl.ShardedJedisExecutorProxy;
import me.binge.redis.utils.RedisCmdPair;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

public class RedisClusterExecutorTest {

    private static volatile boolean run = true;

    public static void test(int ths) {
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort("192.168.2.78", 6379));
        nodes.add(new HostAndPort("192.168.2.78", 6380));
        nodes.add(new HostAndPort("192.168.2.78", 6381));
        nodes.add(new HostAndPort("192.168.2.78", 6382));
        nodes.add(new HostAndPort("192.168.2.78", 6383));
        nodes.add(new HostAndPort("192.168.2.78", 6384));
        ClusterJedisExecutorProxy executorProxy = new ClusterJedisExecutorProxy();
        executorProxy.setCluster(new JedisCluster(nodes, 30 * 2000));
        final RedisExecutor<?> executor = executorProxy.getExecutor();

        ExecutorService executorService = Executors.newFixedThreadPool(ths);
        final Random r = new Random();

        final List<Long> mills = new ArrayList<Long>();
        final AtomicInteger count = new AtomicInteger(0);
        final int seed = ths;
        long totalMills = System.currentTimeMillis();
        final AtomicLong total = new AtomicLong(0);
        while (ths -- > 0) {

            executorService.submit(new Runnable() {

                @Override
                public void run() {
                    while (run) {
                        String i = r.nextInt() + "";
                        long l = System.currentTimeMillis();
                        executor.set(i, i);
                        long m = System.currentTimeMillis() - l;
                        total.incrementAndGet();
//                        System.out.print(">>>" + total.incrementAndGet() + "<<<");
                        if (r.nextInt(seed) % 2 == 0) {
                            if (count.get() >= 3000) {
                                run = false;
                                return;
                            }
                            mills.add(m);
                            System.out.print(count.incrementAndGet() + " ::: ");
                        }
                    }
                }
            });

        }

        while (count.get() < 3000) {
        }
        totalMills = System.currentTimeMillis() - totalMills;
        System.out.println();
        executorService.shutdownNow();
        executorService = null;
        long min = Long.MAX_VALUE;
        long max = -1;
        for (Long m : mills) {
            if (m == null) {
                continue;
            }
            if (m > max) {
                max = m;
            }
            if (m < min) {
                min = m;
            }
        }
        System.out.println("times : " + count.get() + ", ths : " + seed + ", max : " + max + ", min : " + min + ", totalMills : " + totalMills + ", total : " + total.get());
        run = true;
        while (run) {

        }
    }

    public void xxx() {
        Properties props = new Properties();
        RedisExecutor<?> executor = null;

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(Integer.valueOf(props.getProperty("redis.maxidle")));
        config.setMaxTotal(Integer.valueOf(props.getProperty("redis.maxtotal")));
        config.setMaxWaitMillis(Long.valueOf(props.getProperty("redis.maxwait")));
        config.setTestOnBorrow(Boolean.valueOf(props.getProperty("redis.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(props.getProperty("redis.textOnReturn")));
        config.setTestWhileIdle(Boolean.valueOf(props.getProperty("redis.testWhileIdle")));

        Integer model = NumberUtils.createInteger(props.getProperty("redis.model"));
        if (model == null) {
            model = 0;
        }

        Integer timeout = Integer.valueOf(props.getProperty("redis.timeout") == null ? "30000" : props.getProperty("redis.timeout"));
        switch (model) {
        case 0: // 单点模式(简单主从)
            JedisExecutorProxy jedisExecutorProxy = new JedisExecutorProxy();
            JedisPool jedisPool = null;
            try {
                jedisPool = new JedisPool(config, new URI(props.getProperty("redis.single.uri")), timeout);
            } catch (URISyntaxException e) {
                throw new ExceptionInInitializerError("redis.single.uri must like redis://server-ip:port");
            }
            jedisExecutorProxy.setPool(jedisPool);
            executor = jedisExecutorProxy.getExecutor();
            break;
        case 1: // sentinel
            SentinelJedisExecutorProxy sentinelJedisExecutorProxy = new SentinelJedisExecutorProxy();
            HashSet<String> sentinels = new HashSet<String>(getUris(props, "redis.sentinel.hostport."));
            if (sentinels.size() < 1 || sentinels.size() % 2 == 0) { // 初始化时sentinels的主机个数必须是大于1的奇数
                throw new ExceptionInInitializerError("sentinel 模式下初始化时sentinel的服务个数必须是大于1的奇数");
            }
            JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(props.getProperty("redis.sentinel.mastername"), sentinels, config, timeout);
            sentinelJedisExecutorProxy.setPool(jedisSentinelPool);
            executor = sentinelJedisExecutorProxy.getExecutor();
            break;
        case 2: // sharding
            ShardedJedisExecutorProxy shardedJedisExecutorProxy = new ShardedJedisExecutorProxy();
            List<String> uris = getUris(props, "redis.sharded.uri.");
            if (uris == null || uris.isEmpty()) {
                throw new ExceptionInInitializerError("sharded uri不能为空");
            }
            List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
            for (String uri : uris) {
                try {
                    URI su = new URI(uri);
                    shards.add(new JedisShardInfo(su.getHost(), su.getPort(), timeout));
                } catch (URISyntaxException e) {
                    throw new ExceptionInInitializerError("redis.sharded.uri must like redis://server-ip:port");
                }
            }
            ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config, shards);
            shardedJedisExecutorProxy.setPool(shardedJedisPool);
            executor = shardedJedisExecutorProxy.getExecutor();
            break;
        case 3: // cluster
            ClusterJedisExecutorProxy clusterJedisExecutorProxy = new ClusterJedisExecutorProxy();
            uris = getUris(props, "redis.cluster.uri.");
            if (uris == null || uris.isEmpty()) {
                throw new ExceptionInInitializerError("cluster uri不能为空");
            }
            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
            for (String uri : uris) {
                try {
                    URI su = new URI(uri);
                    nodes.add(new HostAndPort(su.getHost(), su.getPort()));
                } catch (URISyntaxException e) {
                    throw new ExceptionInInitializerError("redis.cluster.uri must like redis://server-ip:port");
                }
            }
            JedisCluster jedisCluster = new JedisCluster(nodes, timeout, config);
            clusterJedisExecutorProxy.setCluster(jedisCluster);
            executor = clusterJedisExecutorProxy.getExecutor();
        default:
            throw new ExceptionInInitializerError("不支持的redis model");
        }



    }

    private List<String> getUris(Properties props, String prefix) {
        List<String> uris = new ArrayList<String>();

        Set<String> pks = props.stringPropertyNames();
        for (String pk : pks) {
            if (pk.startsWith(prefix)) {
                uris.add(props.getProperty(pk));
            }
        }
        return uris;
    }


    public static void main(String[] args) {

//        test(100);
//        test(200);
//        test(500);
//        test(1000);

        JedisExecutorProxy proxy = new JedisExecutorProxy();
        JedisPool pool = new JedisPool("127.0.0.1");
        proxy.setPool(pool);
        RedisExecutor<Jedis> executor = proxy.getExecutor();
        Object cmd = executor.cmd(new RedisCmdPair("ping", null));
        System.out.println(cmd);
    }

}
