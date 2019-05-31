package com.huanfion.kafka.intercepter

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}
/*
时间戳拦截器
 */
object TimeInterceptor extends ProducerInterceptor[String,String]{
  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
    //创建一个新的record，把时间戳写进消息体的最前边
    return new ProducerRecord(record.topic(),record.partition(),record.timestamp(),record.key(),
      System.currentTimeMillis()+","+record.value())
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
