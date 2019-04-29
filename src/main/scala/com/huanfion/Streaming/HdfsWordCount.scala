package com.huanfion.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
    val conf=new SparkConf().setAppName("HdfsWordCount")
    val ssc=new StreamingContext(conf,Seconds(2))

    //val lines=ssc.textFileStream(args(0))
    val lines=ssc.socketTextStream("192.168.174.10",9999)
    val words=lines.flatMap(_.split(" "))
    val wordcounts=words.map((_,1)).reduceByKey(_+_)
    wordcounts.print()
    wordcounts.saveAsTextFiles(args(1))
    ssc.start()
    ssc.awaitTermination()
  }
}

