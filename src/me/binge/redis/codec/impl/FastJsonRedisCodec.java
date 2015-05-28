package me.binge.redis.codec.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import me.binge.redis.codec.RedisCodec;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastJsonRedisCodec implements RedisCodec {

    @Override
    public byte[] encode(Object obj) {
        return Charset.forName("utf-8").encode(encodeToStr(obj)).array();
    }

    @Override
    public <T> T decode(byte[] bs) {
        return decodeFromStr(Charset.forName("utf-8").decode(ByteBuffer.wrap(bs)).toString());
    }

    @Override
    public String encodeToStr(Object obj) {
        return JSONObject.toJSONString(obj);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeFromStr(String src) {
        try {
            return (T) JSONObject.parse(src);
        } catch (Exception e) {
            e.printStackTrace();
            if (e instanceof ClassCastException && e.getMessage().contains("com.alibaba.fastjson.JSONArray cannot be cast to com.alibaba.fastjson.JSONObject")) {
                return (T) JSONArray.parse(src);
            }
            throw new RuntimeException(e);
        }
    }

}
