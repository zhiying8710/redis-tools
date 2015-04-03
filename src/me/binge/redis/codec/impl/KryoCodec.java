package me.binge.redis.codec.impl;

import java.io.ByteArrayOutputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import me.binge.redis.codec.RedisCodec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoCodec implements RedisCodec {

    public interface KryoPool {

        Kryo get();

        void yield(Kryo kryo);

    }

    public static class KryoPoolImpl implements KryoPool {

        private final Queue<Kryo> objects = new ConcurrentLinkedQueue<Kryo>();

        public Kryo get() {
            Kryo kryo;
            if ((kryo = objects.poll()) == null) {
                kryo = createInstance();
            }
            return kryo;
        }

        public void yield(Kryo kryo) {
            objects.offer(kryo);
        }

        /**
         * Sub classes can customize the Kryo instance by overriding this method
         *
         * @return create Kryo instance
         */
        protected Kryo createInstance() {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            return kryo;
        }

    }

    public class KryoCodecException extends RuntimeException {

        private static final long serialVersionUID = 9172336149805414947L;

        public KryoCodecException(Throwable cause) {
            super(cause.getMessage(), cause);
            setStackTrace(cause.getStackTrace());
        }
    }

    private final KryoPool kryoPool;

    public KryoCodec() {
        this(new KryoPoolImpl());
    }

    public KryoCodec(KryoPool kryoPool) {
        this.kryoPool = kryoPool;
    }


    @Override
    public byte[] encode(Object obj) {
        Kryo kryo = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output output = new Output(baos);
            kryo = kryoPool.get();
            kryo.writeClassAndObject(output, obj);
            output.close();
            return baos.toByteArray();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new KryoCodecException(e);
        } finally {
            if (kryo != null) {
                kryoPool.yield(kryo);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(byte[] bs) {
        Kryo kryo = null;
        try {
            kryo = kryoPool.get();
            return (T) kryo.readClassAndObject(new Input(bs));
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new KryoCodecException(e);
        } finally {
            if (kryo != null) {
                kryoPool.yield(kryo);
            }
        }
    }

    @Override
    public String encodeToStr(Object obj) {
        return new String(encode(obj), CHARSET);
    }

    @Override
    public <T> T decodeFromStr(String src) {
        return decode(src.getBytes(CHARSET));
    }

}
