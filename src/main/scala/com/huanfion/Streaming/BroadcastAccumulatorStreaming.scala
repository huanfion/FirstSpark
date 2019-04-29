package com.huanfion.Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object BroadcastAccumulatorStreaming {
  /*
  * 声明一个广播变量和一个累加器
  * "use AccumulatorV2", "2.0.0"
  * */
  private var broadcastList: Broadcast[List[String]] = _
  private var accumulator: LongAccumulator = _

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("BroadcastAccumulatorStreaming").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    broadcastList = ssc.sparkContext.broadcast(List("Hadoop", "Spark"))
    accumulator = ssc.sparkContext.longAccumulator("AccumulatorTest")

    /*
    * 读取数据*/
    val lines = ssc.socketTextStream("master", 9999)

    val words = lines.flatMap(_.split(" ")).map((_, 1))
    System.out.println(broadcastList.value)
    val wordcount = words.filter(x => {broadcastList.value.contains(x._1)}).reduceByKey(_ + _)

    wordcount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
