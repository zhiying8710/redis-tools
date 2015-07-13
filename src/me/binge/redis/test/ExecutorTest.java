package me.binge.redis.test;

import java.io.InputStream;
import java.util.Properties;

import me.binge.redis.exec.RedisExecutor;
import me.binge.redis.exec.RedisExecutors;

public class ExecutorTest {

    public static void main(String[] args) throws Exception {

        InputStream in = ExecutorTest.class.getClassLoader().getResourceAsStream("sample-conf.properties");
        Properties props = new Properties();
        props.load(in);
        in.close();

        RedisExecutor<?> executor = RedisExecutors.get(props);
        executor.set("__hahah", "gogogogo");
        System.out.println(executor.get("__hahah"));
        executor.close();


    }


}
