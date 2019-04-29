package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔积
  */
object Cartesian {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd1 = sc.parallelize(Array("衣服1", "衣服2"))
    val rdd2 = sc.parallelize(Array("裤子1", "裤子2", "裤子3"))
    val cartesianrdd =rdd1.cartesian(rdd2)

    cartesianrdd.collect().foreach(x=>{println(x._1,x._2)})
    sc.stop()
  }
}
