package com.huanfion.Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReceiverFromKafka {
  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setAppName("ReceiverFromkafka").setMaster("local[*]")

      val sc=new SparkContext(sparkConf)

      val ssc=new StreamingContext(sc,Seconds(5))
  }
}
