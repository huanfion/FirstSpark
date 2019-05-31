package com.huanfion.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
Kafka 消费者API
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //Kafka服务端的主机名和端口号
        pros.put("bootstrap.servers", "master:9092");
        //指定consumer group
        pros.put("group.id", "test");
        //是否自动确认offset
        pros.put("enable.auto.commit", "true");
        //自动确认offset的间隔时间
        pros.put("auto.commit.interval.ms", "1000");
        //key的反序列化操作
        pros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //value的反序列化操作
        pros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //消费者订阅的topic 可同时订阅多个
        consumer.subscribe(Arrays.asList("first", "second"));
       while (true){
            ConsumerRecords<String,String> records=consumer.poll(100);
           for (ConsumerRecord<String, String> record:records) {
               System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
           }
       }
    }
}
