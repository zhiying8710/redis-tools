package me.binge.redis.exec;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import me.binge.redis.exec.impl.ClusterJedisExecutorProxy;
import me.binge.redis.exec.impl.JedisExecutorProxy;
import me.binge.redis.exec.impl.SentinelJedisExecutorProxy;
import me.binge.redis.exec.impl.ShardedJedisExecutorProxy;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

public class RedisExecutors {

    private static final Logger logger = Logger.getLogger(RedisExecutors.class);

    public static RedisExecutor<?> get(Properties props) {

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(Integer.valueOf(props.getProperty("redis.maxidle",
                "10")));
        config.setMaxTotal(Integer.valueOf(props.getProperty("redis.maxtotal",
                "500")));
        config.setMaxWaitMillis(Long.valueOf(props.getProperty("redis.maxwait",
                "30000")));
        config.setTestOnBorrow(Boolean.valueOf(props.getProperty(
                "redis.testOnBorrow", "true")));
        config.setTestOnReturn(Boolean.valueOf(props.getProperty(
                "redis.textOnReturn", "true")));
        config.setTestWhileIdle(Boolean.valueOf(props.getProperty(
                "redis.testWhileIdle", "true")));

        RedisExecutor<?> executor = null;

        Integer model = NumberUtils.createInteger(props
                .getProperty("redis.model"));
        if (model == null) {
            model = 0;
        }

        Integer timeout = Integer
                .valueOf(props.getProperty("redis.timeout") == null ? "30000"
                        : props.getProperty("redis.timeout"));
        switch (model) {
        case 0: // 单点模式(简单主从)
            JedisExecutorProxy jedisExecutorProxy = new JedisExecutorProxy();
            JedisPool jedisPool = null;
            try {
                jedisPool = new JedisPool(config, new URI(
                        props.getProperty("redis.single.uri")), timeout);
            } catch (URISyntaxException e) {
                throw new ExceptionInInitializerError(
                        "redis.single.uri must like redis://server-ip:port");
            }
            jedisExecutorProxy.setPool(jedisPool);
            executor = jedisExecutorProxy.getExecutor();
            logger.info("初始化RedisCacher, Redis使用单点模式(简单主从)");
            break;
        case 1: // sentinel
            SentinelJedisExecutorProxy sentinelJedisExecutorProxy = new SentinelJedisExecutorProxy();
            HashSet<String> sentinels = new HashSet<String>(getUris(props,
                    "redis.sentinel.hostport."));
            if (sentinels.size() < 1 || sentinels.size() % 2 == 0) { // 初始化时sentinels的主机个数必须是大于1的奇数
                throw new ExceptionInInitializerError(
                        "sentinel 模式下初始化时sentinel的服务个数必须是大于1的奇数");
            }
            JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(
                    props.getProperty("redis.sentinel.mastername"), sentinels,
                    config, timeout);
            sentinelJedisExecutorProxy.setPool(jedisSentinelPool);
            executor = sentinelJedisExecutorProxy.getExecutor();
            logger.info("初始化RedisCacher, Redis使用sentinel(哨兵)模式");
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
                    shards.add(new JedisShardInfo(su.getHost(), su.getPort(),
                            timeout));
                } catch (URISyntaxException e) {
                    throw new ExceptionInInitializerError(
                            "redis.sharded.uri must like redis://server-ip:port");
                }
            }
            ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config,
                    shards);
            shardedJedisExecutorProxy.setPool(shardedJedisPool);
            executor = shardedJedisExecutorProxy.getExecutor();
            logger.info("初始化RedisCacher, Redis使用sharding(分片)模式");
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
                    throw new ExceptionInInitializerError(
                            "redis.cluster.uri must like redis://server-ip:port");
                }
            }
            JedisCluster jedisCluster = new JedisCluster(nodes, timeout, config);
            clusterJedisExecutorProxy.setCluster(jedisCluster);
            executor = clusterJedisExecutorProxy.getExecutor();
            logger.info("初始化RedisCacher, Redis使用cluster(集群)模式");
        default:
            throw new ExceptionInInitializerError("不支持的redis model");
        }

        return executor;
    }

    private static List<String> getUris(Properties props, String prefix) {
        List<String> uris = new ArrayList<String>();
        Set<Object> pks = props.keySet();
        for (Object pk : pks) {
            if (pk.toString().startsWith(prefix)) {
                uris.add(props.getProperty(pk.toString()));
            }
        }
        return uris;
    }

}
