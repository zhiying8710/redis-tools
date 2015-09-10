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

import org.apache.commons.lang3.StringUtils;
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
        String sTrue = Boolean.TRUE.toString();
        config.setTestOnBorrow(Boolean.valueOf(props.getProperty(
                "redis.testOnBorrow", sTrue)));
        config.setTestOnReturn(Boolean.valueOf(props.getProperty(
                "redis.textOnReturn", sTrue)));
        config.setTestWhileIdle(Boolean.valueOf(props.getProperty(
                "redis.testWhileIdle", sTrue)));

        String passwd = props.getProperty("redis.password");
        passwd = StringUtils.isBlank(passwd) ? null : passwd;
        int db = Integer.valueOf(props.getProperty("redis.db", "0"));

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

            JedisPool jedisPool = null;
            List<String> uris = getUris(props, "redis.single.uri");
            if (uris == null || uris.isEmpty()) {
                throw new ExceptionInInitializerError("single uri can not be null");
            }
            try {
                URI uri = new URI(uris.get(0));
                jedisPool = new JedisPool(config, uri.getHost(), uri.getPort(), timeout, passwd, db);
            } catch (URISyntaxException e) {
                throw new ExceptionInInitializerError(
                        "redis.single.uri must like redis://server-ip:port");
            }
            executor = new JedisExecutorProxy(jedisPool).getExecutor();
            logger.info("Init RedisExecutor, Redis use single node model.");
            break;
        case 1: // sentinel
            HashSet<String> sentinels = new HashSet<String>(getUris(props,
                    "redis.sentinel.hostport."));
            if (sentinels.size() < 1 || sentinels.size() % 2 == 0) { // 初始化时sentinels的主机个数必须是大于1的奇数
                throw new ExceptionInInitializerError(
                        "in sentinel model, sentinel's hosts count must more than 1 and it's must be a odd.");
            }
            JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(
                    props.getProperty("redis.sentinel.mastername"), sentinels,
                    config, timeout, passwd, db);
            executor = new SentinelJedisExecutorProxy(jedisSentinelPool).getExecutor();
            logger.info("Init RedisExecutor, Redis use sentinel model.");
            break;
        case 2: // sharding
            uris = getUris(props, "redis.sharded.uri.");
            if (uris == null || uris.isEmpty()) {
                throw new ExceptionInInitializerError("sharded uris can not bu null");
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
            executor = new ShardedJedisExecutorProxy(shardedJedisPool).getExecutor();
            logger.info("Init RedisExecutor, Redis use sharded model.");
            break;
        case 3: // cluster
            uris = getUris(props, "redis.cluster.uri.");
            if (uris == null || uris.isEmpty()) {
                throw new ExceptionInInitializerError("cluster uris can not be null.");
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
            executor = new ClusterJedisExecutorProxy(jedisCluster).getExecutor();
            logger.info("Init RedisExecutor, Redis use cluster model.");
        default:
            throw new ExceptionInInitializerError("unsupported redis model.");
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
