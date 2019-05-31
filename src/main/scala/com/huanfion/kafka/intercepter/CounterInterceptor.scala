package com.huanfion.kafka.intercepter

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

/*
统计发送消息成功和发送消息失败数
 */
object CounterInterceptor extends ProducerInterceptor[String, String] {
  private var errorCounter = 0
  private var successCounter = 0

  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
    record
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception == null) {
      successCounter += 1
    }
    else {
      errorCounter += 1
    }
  }

  override def close(): Unit = {
    println("successful send:" + successCounter)
    println("Failed send:" + errorCounter)
  }

  override def configure(configs: util.Map[String, _]): Unit = ???
}
