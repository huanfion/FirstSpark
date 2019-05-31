package com.huanfion.kafka.producer;


import com.huanfion.kafka.producer.IntegerSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/*
kafak 生产者java API
 */
public class MyProducer {

    public static void main(String[] args) {
        Properties pros = new Properties();
        //Kafka服务端的主机名和端口号
        pros.put("bootstrap.servers", "master:9092");
        //等待所有副本节点的应答
        pros.put("acks", "all");
        //消息发送的最大尝试数
        pros.put("retries", 0);
        //一批消息处理大小
        pros.put("batch.size", 16384);
        //请求延时
        pros.put("linger.ms", 1);
        //发送缓存区内存大小
        pros.put("buffer.memory", 33554432);
        //key序列化
       // 0.8.2这个版本还没有 org.apache.kafka.common.serialization.Serializer.IntegerSerializer，在2.2.0版本就有这个类
        //那没有怎么办呢，只能自己去实现咯、
        pros.put("key.serializer", IntegerSerializer.class);
        //value序列化
        pros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(pros);
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<Integer, String>("first", i, "hello world(int)-" + i));
        }
        producer.close();
    }
}
