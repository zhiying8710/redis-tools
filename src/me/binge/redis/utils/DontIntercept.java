package me.binge.redis.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * use for class proxy, if this annotation on a method, means this method will not be intercept(dont do any aop thing)
 * @author Admin
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface DontIntercept {

}
