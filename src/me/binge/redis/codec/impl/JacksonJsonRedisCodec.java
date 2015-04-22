package me.binge.redis.codec.impl;

import java.util.Arrays;

import me.binge.redis.codec.RedisCodec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;


public class JacksonJsonRedisCodec implements RedisCodec {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ObjectMapper mapObjectMapper = new ObjectMapper();

    public JacksonJsonRedisCodec() {
        init(objectMapper);
        TypeResolverBuilder<?> typer = new DefaultTypeResolverBuilder(DefaultTyping.NON_FINAL);
        typer.init(JsonTypeInfo.Id.CLASS, null);
        typer.inclusion(JsonTypeInfo.As.PROPERTY);
        objectMapper.setDefaultTyping(typer);

        init(mapObjectMapper);
        // type info inclusion
        TypeResolverBuilder<?> mapTyper = new DefaultTypeResolverBuilder(DefaultTyping.NON_FINAL) {
            private static final long serialVersionUID = 1L;

            public boolean useForType(JavaType t)
            {
                switch (_appliesFor) {
                case NON_CONCRETE_AND_ARRAYS:
                    while (t.isArrayType()) {
                        t = t.getContentType();
                    }
                    // fall through
                case OBJECT_AND_NON_CONCRETE:
                    return (t.getRawClass() == Object.class) || !t.isConcrete();
                case NON_FINAL:
                    while (t.isArrayType()) {
                        t = t.getContentType();
                    }
                    // to fix problem with wrong long to int conversion
                    if (t.getRawClass() == Long.class) {
                        return true;
                    }
                    return !t.isFinal(); // includes Object.class
                default:
                //case JAVA_LANG_OBJECT:
                    return (t.getRawClass() == Object.class);
                }
            }
        };
        mapTyper.init(JsonTypeInfo.Id.CLASS, null);
        mapTyper.inclusion(JsonTypeInfo.As.PROPERTY);
        mapObjectMapper.setDefaultTyping(mapTyper);
    }

    protected void init(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.setVisibilityChecker(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
                                            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                                            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                                            .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                                            .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true).configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    public static class ThrowableWrapper {

        private String type;
        private String message;
        private StackTraceElement[] elements;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public StackTraceElement[] getElements() {
            return elements;
        }

        public void setElements(StackTraceElement[] elements) {
            this.elements = elements;
        }

        @Override
        public String toString() {
            return "ThrowableWapper [type=" + type + ", message=" + message
                    + ", elements=" + Arrays.toString(elements) + "]";
        }

    }

    @Override
    public byte[] encode(Object obj) {
        try {

            if (obj instanceof Throwable) {
                obj = throwableWrap((Throwable) obj);
            }

            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(byte[] bs) {
        try {
            return (T) objectMapper.readValue(bs, Object.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String encodeToStr(Object obj) {
        try {

            if (obj instanceof Throwable) {
                obj = throwableWrap((Throwable) obj);
            }

            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeFromStr(String src) {
        try {
            return (T) objectMapper.readValue(src, Object.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * because when encode a throwable instance, it will throw exceptions, so need to wrap the instance.
     * @param obj
     * @return
     */
    private ThrowableWrapper throwableWrap(Throwable obj) {
        ThrowableWrapper wapper = new ThrowableWrapper();
        wapper.setType(obj.toString());
        wapper.setMessage(obj.getMessage());
        wapper.setElements(obj.getStackTrace());
        return wapper;
    }

}
