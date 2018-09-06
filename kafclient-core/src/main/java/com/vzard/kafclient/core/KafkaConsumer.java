package com.vzard.kafclient.core;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class KafkaConsumer {

    private String propertiesFile;

    private Properties properties;

    private String topic;

    private int streamNum;


}
