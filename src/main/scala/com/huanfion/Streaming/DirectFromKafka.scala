package com.huanfion.Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DirectFromKafka {
  def main(args: Array[String]): Unit = {
    if(args.length<4){
      System.err.println("DirectFromKafka need :<zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)//System.exit(0)表示正常退出，1表示非正常退出
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val Array(zkQuorum,group,topics,numThreads)=args;
    val sparkConf=new SparkConf().setAppName("DirectKafka")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap

    //    val lines=KafkaUtils.createStream()
    //    val words=lines.flatMap(_.split(" "))
    //    val wordCounts=words.map((_,1L)).reduceByKeyAndWindow(_+_,_-_,Minutes(10),Seconds(5),2)
    //
    //    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
