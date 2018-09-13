package com.vzard.kafclient.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


import java.io.IOException;
import java.util.*;

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

    /**
     * 发送string类型
     *
     * @param topicName
     * @param message
     */
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


    public void send(String key, String message) {
        send2Topic(null, key, message);
    }

    /**
     * 发送key-value型信息
     *
     * @param topicName
     * @param key
     * @param message
     */
    public void send2Topic(String topicName, String key, String message) {
        if (message == null) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }
        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, key, message);
        producer.send(km);
    }


    public void send(Collection<String> message) {
        send2Topic(null, message);
    }

    /**
     * 发送集合类型的消息
     *
     * @param topicName
     * @param messages
     */
    public void send2Topic(String topicName, Collection<String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        if (topicName == null) {
            topicName = defaultTopic;
        }

        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
        int i = 0;
        for (String entry : messages) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, entry);
            kms.add(km);
            i++;
            //Send the message 20
            if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
                producer.send(kms);
                kms.clear();
            }
        }

        if (!kms.isEmpty()) {
            producer.send(kms);
        }

    }


    public void send(Map<String, String> messages) {
        send2Topic(null, messages);
    }

    /**
     * 发送map类型的消息
     *
     * @param topicName
     * @param messages
     */
    public void send2Topic(String topicName, Map<String, String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        if (topicName == null) {
            topicName = defaultTopic;
        }

        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();

        int i = 0;
        for (Map.Entry<String, String> entry : messages.entrySet()) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, entry.getKey(), entry.getValue());
            kms.add(km);
            i++;

            // Send the messages 20 at most once
            if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
                producer.send(kms);
                kms.clear();
            }

            if (!kms.isEmpty()) {
                producer.send(kms);
            }
        }

    }

    // Send bean messages
    public <T> void sendBean(T bean) {

    }


    public <T> void sendBeans(Collection<T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Collection<T> beans) {
        Collection<String> beanStr = new ArrayList<>();
        for (T bean : beans) {
            beanStr.add(JSON.toJSONString(bean));
        }
        send2Topic(topicName, beanStr);
    }

    public <T> void sendBeans(Map<String, T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Map<String, T> beans) {
        Map<String, String> beansStr = new HashMap<>();
        for (Map.Entry<String, T> entry : beans.entrySet()) {
            beansStr.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
        }
        send2Topic(topicName, beansStr);
    }

    // send JSON Object message

    public void sendObjects2Topic(String topicName, Map<String, JSONObject> jsonObjects) {
        Map<String, String> objectsStrs = new HashMap<>();
        for (Map.Entry<String, JSONObject> entry : jsonObjects.entrySet()) {
            objectsStrs.put(entry.getKey(), entry.getValue().toJSONString());
        }
        send2Topic(topicName, objectsStrs);
    }

    public void sendObjects(Map<String, JSONObject> jsonObjectMap) {
        sendObjects2Topic(null, jsonObjectMap);
    }

    public void sendObjects2Topic(String topicName, JSONArray jsonArray) {
        send2Topic(topicName, jsonArray.toJSONString());
    }

    public void sendObjects(JSONArray jsonArray) {
        sendObjects2Topic(null, jsonArray);
    }

    public void sendObject2Topic(String topicName, String key, JSONObject jsonObject) {
        send2Topic(topicName, key, jsonObject.toJSONString());
    }

    public void sendObject(String key, JSONObject jsonObject) {
        sendObject2Topic(null, key, jsonObject);
    }

    public void sendObject2Topic(String topicName, JSONObject jsonObject) {
        send2Topic(topicName, jsonObject.toJSONString());
    }

    public void sendObject(JSONObject jsonObject) {
        sendObject2Topic(null, jsonObject);
    }


    public void close() {
        producer.close();
    }


}

