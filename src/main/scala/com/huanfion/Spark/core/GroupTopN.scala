package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * GroupTopN实践
  */
object GroupTopN {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("GroupTopN")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val list=Array(
      "class1 67", "class2 89", "class1 78", "class2 90",
      "class1 99", "class3 34", "class3 89","class1 79",
      "class2 98","class2 33")
    val rdd=sc.parallelize(list,1)
    println(rdd.partitions.length)
    val beginGroup=rdd.map(x=>(x.split(" ")(0),x.split(" ")(1).toInt))
      .groupByKey()
    val TopValues=beginGroup.map(x=>{
      val values=x._2.toList.sortWith(_<_).take(2)
      (x._1,values)
    })
    TopValues.foreach(x=>{
      println(x._1)
      x._2.foreach(y=>println(y))
    })
  }
}
