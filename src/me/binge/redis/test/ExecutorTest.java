package me.binge.redis.test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

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

        Set<String> nums = executor.zrangeByScore("__order_3D8398C8EDBA6D55609B388B69C52A5B_266589", Double.MIN_VALUE, Double.MAX_VALUE);
        boolean isOrder = true;
        int last = -1;
        for (String num : nums) {
            int curr = Integer.valueOf(num);
            if (last == -1) {
                last = curr;
                continue;
            }
            if (curr < last) {
                System.out.println(curr + ":" + last);
                isOrder = false;
                break;
            }
            last = curr;
        }
        System.err.println(isOrder);

        executor.close();


    }


}
