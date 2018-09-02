package com.vzard.kafclient.boot;

import java.lang.annotation.*;

/**
 * Created with IntelliJ IDEA.
 * User: vzard
 * Date: 2018/9/2
 * Time: 23:25
 * To change this template use File | Settings | File Templates.
 * Description: mark a method as a exception handler and includes the metadata for exception handler
 **/
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ErrorHandler {
    Class<? extends Throwable> exception() default Throwable.class;

    String topic() default "";

}
