package com.vzard.kafclient.core;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


import java.io.IOException;
import java.util.Properties;

@Slf4j
@Getter
@Setter
public class KafkaProducer {

    protected static int MULTI_MSG_ONCE_SEND_NUM = 20;

    private Producer<String, String> producer;

    private String defaultTopic;

    private String propertiesFile;

    private Properties properties;


    public KafkaProducer() {

    }

    public KafkaProducer(String defaultTopic, String propertiesFile) {
        this.defaultTopic = defaultTopic;
        this.propertiesFile = propertiesFile;

        init();
    }

    public KafkaProducer(String defaultTopic, Properties properties) {
        this.defaultTopic = defaultTopic;
        this.properties = properties;

        init();
    }


    protected void init() {

        if (properties == null) {
            properties = new Properties();
            try {
                //加载配置文件
                properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile));
            } catch (IOException e) {
                log.error("he properties file is not loaded.");
                throw new IllegalArgumentException("The properties file is not loaded.", e);
            }

        }
        log.info("Producer properties: " + properties);
        //用配置文件构造生产者配置
        ProducerConfig producerConfig = new ProducerConfig(properties);
        //用生产者配置构造生产者
        producer = new Producer<String, String>(producerConfig);

    }


    public void send(String message) {
        send2Topic(null, message);
    }


    public void send2Topic(String topicName, String message) {
        if (message == null) {
            return;
        }
        if (topicName == null) {
            topicName = defaultTopic;
        }

        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message);
        producer.send(km);
    }


}

