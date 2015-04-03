package me.binge.redis.codec;

import java.nio.charset.Charset;

public interface RedisCodec {

    public static final Charset CHARSET = Charset.forName("utf-8");

    byte[] encode(Object obj);

    <T> T decode(byte[] bs);

    String encodeToStr(Object obj);

    <T> T decodeFromStr(String src);
}
