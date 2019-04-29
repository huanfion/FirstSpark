package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相对于mapPartitions，mapPartitionswithIndex多了一个index（索引）
  */
object MapPartitionWithindex {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  /**
    * mappartitionwithindex 输入函数
    * @param i
    * @param iter
    * @return
    */
  def funmappartindex(i:Int,iter: Iterator[String]):Iterator[String]={
    val res=for(e<-iter) yield e+"_"+i
    res
  }
  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd=sc.parallelize(Array("henry","cherry","leo","ben"),2)
    val indexrdd=rdd.mapPartitionsWithIndex(funmappartindex)

    indexrdd.foreach(x=>println(x))
  }
}
