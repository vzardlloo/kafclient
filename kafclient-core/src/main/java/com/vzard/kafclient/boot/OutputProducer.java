package com.vzard.kafclient.boot;

import java.lang.annotation.*;

/**
 * Created with IntelliJ IDEA.
 * User: vzard
 * Date: 2018/9/2
 * Time: 23:46
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OutputProducer {

    String propertiesFile() default "";

    String defaultTopic() default "";


}
