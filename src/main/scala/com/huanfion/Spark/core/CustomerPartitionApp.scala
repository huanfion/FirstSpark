package com.huanfion.Spark.core

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

/*
* 自定义分区
 */
class CustomerPartitioner(numParts: Int) extends Partitioner {
  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length - 1).toInt % numParts
  }
}

object CustomerPartitionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CustomerPartitionsAPP").master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.makeRDD(List("aa.2", "bb.2", "cc.3", "dd.3", "ee.5"))
    rdd.map((_, 1)).partitionBy(new CustomerPartitioner(3)).mapPartitionsWithIndex((index,iter)=>Iterator(index.toString+":"+iter.mkString("|")))
      .foreach(x=>println(x))
  }
}
