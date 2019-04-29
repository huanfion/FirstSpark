package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Sample的使用
  */
object Sample {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }
  def main(args: Array[String]): Unit = {
    val sc=getsc()
    val rdd = sc.parallelize(Array("hello","jason","jim","vin","test","mary"))
    val samplerdd=rdd.sample(false,0.3)

    samplerdd.foreach(x=>println(x))
  }
}
