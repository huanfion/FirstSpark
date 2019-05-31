package com.huanfion.kafka.intercepter

import java.util
import java.util.{Arrays, Properties}

import com.fasterxml.jackson.databind.ser.std.StdKeySerializers.StringKeySerializer
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

object InterceptorProducer {
  def main(args: Array[String]): Unit = {
    //设置配置信息
    val props = new Properties()
    props.put("bootstrap.servers","master:9092")
    props.put("ack","all")
    props.put("retries","0")
    props.put("batch.size","16384")
    props.put("linger.ms","1")
    //发送缓存区内存大小
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    //value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    //构建拦截链
    val interceptors= util.Arrays.asList("com.huanfion.kafka.intercepter.JavaTimeInterceptor","com.huanfion.kafka.intercepter.JavaCounterInterceptor")
    //util.Arrays.asList("com.huanfion.kafka.intercepter.TimeInterceptor","com.huanfion.kafka.intercepter.CounterInterceptor")
    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors)
    val producer=new KafkaProducer[String, String](props)
    //发送消息
    for(i<-Range(1,40)){
      producer.send(new ProducerRecord[String,String]("first", "hello world"+i))
    }
    // 4 一定要关闭producer，这样才会调用interceptor的close方法
    producer.close()
  }
}
