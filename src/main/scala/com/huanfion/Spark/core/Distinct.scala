package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Distinct")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(Array("hello", "hello", "jim", "vin", "test", "mary"))
    rdd.distinct().foreach(x => println(x))
    sc.stop()
  }
}
