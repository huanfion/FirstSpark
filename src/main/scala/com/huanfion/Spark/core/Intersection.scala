package com.huanfion.Spark.core

import com.huanfion.Spark.core.Sample.getsc
import org.apache.spark.{SparkConf, SparkContext}

object Intersection {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd1 = sc.parallelize(Array("hello", "jason", "jim", "vin", "test", "mary"))
    val rdd2 = sc.parallelize(Array("hello", "world"))

    val intesctionrdd = rdd1.intersection(rdd2)

    intesctionrdd.foreach(x => println(x))
  }

}
