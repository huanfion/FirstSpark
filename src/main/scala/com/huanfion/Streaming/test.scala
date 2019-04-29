package com.huanfion.Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Soft\\hadoop-common-2.2.0-bin-master");
    /**
      * 第1步；创建Spark的配置对象SparkConf，设置Spark程序运行时的配置信息
      * 例如 setAppName用来设置应用程序的名称，在程序运行的监控界面可以看到该名称，
      * setMaster设置程序运行在本地还是运行在集群中，运行在本地可是使用local参数，也可以使用local[K]/local[*],
      * 可以去spark官网查看它们不同的意义。 如果要运行在集群中，以Standalone模式运行的话，需要使用spark://HOST:PORT
      * 的形式指定master的IP和端口号，默认是7077
      */
    val conf=new SparkConf().setAppName("wordcount").setMaster("local[2]")
    //    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://master:7077")  // 运行在集群中
    val ssc=new StreamingContext(conf,Seconds(2))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val lines=ssc.socketTextStream("192.168.106.128",9999)
    val words=lines.flatMap(_.split(" "))
    //sparkstreaming wordcount
    //val wordcounts=words.map((_,1L))reduceByKey(_+_)
    //窗口函数
    //    val wordcounts=words.map((_,1))
    //     .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(2))
    //统计量
    val addFunc=(curValues:Seq[Long],preValueState:Option[Long])=>{
      val curCount=curValues.sum
      val preCount=preValueState.getOrElse(0L)
      Some(curCount+preCount)
    }
    //使用updateStateByKey 需要设置checkpoint
    ssc.checkpoint("D:\\DATA\\checkpoint")
    val wordcounts=words.map((_,1L)).updateStateByKey[Long](addFunc)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()


  }
}