package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/** *
  * 将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
  */
object glom {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc=getsc()
    var rdd=sc.makeRDD(1 to 10 ,2)
    var glomrdd=rdd.glom()
    glomrdd.foreach(x=>{
      for(e<-x) print(e +" ")
    })
  }
}
