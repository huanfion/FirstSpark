package com.huanfion.Spark.project

import org.apache.spark.{SparkConf, SparkContext}

object LogAppScala {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setAppName("SecondSort").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.textFile("hdfs://master:9000/data/access.log")
    // RDD[(String, LogSortScala)]
    val logmaprdd=rdd.map(x => (x.split("\t")(1),
      new LogSortScala(x.split("\t")(0).toLong,x.split("\t")(2).toLong,x.split("\t")(3).toLong)))
    val groupRDD=logmaprdd.reduceByKey((x,y)=>{
      var timestamp=if (x.timeStamp<y.timeStamp) x.timeStamp else y.timeStamp
      var uptraffic=x.upTraffic+y.upTraffic
      var downtraffic=x.downTraffic+y.downTraffic
      val logsort=new LogSortScala(timestamp,uptraffic,downtraffic);
      logsort
    })
    val logSortrdd=groupRDD.map(x=>(x._2,x._1)).sortByKey(false)
    logSortrdd.take(10).foreach(x=>{
      println(x._2+":"+x._1.upTraffic+":"+x._1.downTraffic)
    })
    sc.stop()
  }
}

class LogSortScala(val timeStamp: Long, val upTraffic: Long, val downTraffic: Long) extends Ordered[LogSortScala] with Serializable {
  override def compare(that: LogSortScala): Int = {
    var comp = this.upTraffic.toLong.compareTo(that.upTraffic.toLong)
    if (comp == 0) {
      comp = this.downTraffic.toLong.compareTo(that.downTraffic.toLong)
    }
    if (comp == 0) {
      comp = this.timeStamp.toLong.compareTo(that.timeStamp.toLong)
    }
    return comp
  }
}