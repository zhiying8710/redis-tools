package me.binge.redis.utils;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.MethodUtils;


public class EvolutionMethodUtils extends MethodUtils {

    public static Object invokeMethod(final Object object,
            final String methodName, Object... args)
            throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        args = ArrayUtils.nullToEmpty(args);
        final Class<?>[] parameterTypes = toClass(args);
        return invokeMethod(object, methodName, args, parameterTypes);
    }

    public static Class<?>[] toClass(final Object... array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return ArrayUtils.EMPTY_CLASS_ARRAY;
        }
        final Class<?>[] classes = new Class[array.length];
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null) {
                classes[i] = null;
                continue;
            }
            if (array[i] instanceof Integer) {
                classes[i] = Integer.TYPE;
            } else if (array[i] instanceof Byte) {
                classes[i] = Byte.TYPE;
            } else if (array[i] instanceof Short) {
                classes[i] = Short.TYPE;
            } else if (array[i] instanceof Float) {
                classes[i] = Float.TYPE;
            } else if (array[i] instanceof Double) {
                classes[i] = Double.TYPE;
            } else if (array[i] instanceof Character) {
                classes[i] = Character.TYPE;
            } else if (array[i] instanceof Long) {
                classes[i] = Long.TYPE;
            } else if (array[i] instanceof Boolean) {
                classes[i] = Boolean.TYPE;
            } else {
                classes[i] = array[i].getClass();
            }
        }
        return classes;
    }
}
