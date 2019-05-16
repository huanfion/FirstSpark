package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/** *
  * MapPartition 是map的一个变种，它们都可以进行分区的并行处理
  * 二者粒度不一样 map是对应rdd每一个元素 mappartitions输入的函数应用于每个分区
  */
object MapPartitions {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  /**
    * 定义一个map输入函数 作用于rdd中每一个元素 翻倍
    *
    * @param e
    * @return
    */
  def funPerElement(e: Int) = {
    println("e:" + e)
    e * 2
  }

  /*
  * mapPartitions的输入函数。iter是分区中元素的迭代子，返回类型也要是迭代子
   */
  def funPerPartition(iter: Iterator[Int]): Iterator[Int] = {
    println("run in partiton")
    val res = for (e <- iter) yield e * 2
    res
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(1 to 10, 3)

    val maprdd = rdd.map(funPerElement)
    maprdd.collect()
    val mapPartitionrdd = rdd.mapPartitions(funPerPartition)
    mapPartitionrdd.collect().foreach(x => println(x))
    sc.stop()
  }

}
