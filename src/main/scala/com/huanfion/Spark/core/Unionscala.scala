package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Union的使用
  */
object Unionscala {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc=getsc()
    val rdd1=sc.parallelize(Array("cherry","herry"))
    var rdd2=sc.parallelize(Array("leo","ben","lili"))

    val unionrdd=rdd1.union(rdd2)
    unionrdd.foreach(x=>println(x))
  }
}
