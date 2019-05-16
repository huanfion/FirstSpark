package com.huanfion.Spark.core

import com.huanfion.Spark.core.Unionscala.getsc
import org.apache.spark.{SparkConf, SparkContext}

object Subtractscala {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc=getsc()
    val rdd1=sc.parallelize(Array("cherry","herry","leo"))
    var rdd2=sc.parallelize(Array("leo","ben","lili"))

    val unionrdd=rdd1.subtract(rdd2)
    unionrdd.foreach(x=>println(x))
  }
}
