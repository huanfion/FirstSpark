package com.huanfion.Streaming

import java.sql.DriverManager

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SvaeToMysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("savetomysql")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("master", 9999)
    val wordcounts = lines.flatMap(x => x.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordcounts.foreachRDD(rdd => rdd.foreachPartition(line => {
      Class.forName("com.mysql.jdbc.Driver")
      //获取mysql连接
      val conn = DriverManager.getConnection("jdbc:mysql://master:3306/test", "root", "123456")
      //把数据写入mysql
      try {
        for (row <- line) {
          val sql = "insert into wordcount(titleName,count)values('" + row._1 + "','" + row._2 + "')"
          conn.prepareStatement(sql).executeUpdate()
        }
      } finally {
        conn.close()
      }
    }))

    ssc.start()
    ssc.awaitTermination()

  }
}