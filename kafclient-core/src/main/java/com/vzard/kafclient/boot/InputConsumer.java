package com.vzard.kafclient.boot;

import java.lang.annotation.*;

/**
 * Created with IntelliJ IDEA.
 * User: vzard
 * Date: 2018/9/2
 * Time: 23:38
 * To change this template use File | Settings | File Templates.
 * Description:
 **/

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InputConsumer {

    String propertisFile() default "";

    String topic() default "";

    int streamNum() default 1;

    int fixedThreadNum() default 0;

    int minThreadNum() default 0;

    int maxThreadNum() default 0;


}
