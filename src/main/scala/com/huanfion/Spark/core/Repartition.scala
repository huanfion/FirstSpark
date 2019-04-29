package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Repartition算子将任意RDD的partition数量增大或减小，与coalesce不同的是，
  * coalesce一般只能将rdd的partition数量减少
  */
object Repartition {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(Array("hello","jason","jim","vin","test","mary"),5)
    println("repartition 前的partition个数"+rdd.partitions.length)
    val mapIndexValue=rdd.mapPartitionsWithIndex((index,y)=>{
      val res=for(e<-y) yield index+":"+e
      res
    })
    mapIndexValue.foreach(x=>{println(x)})

    val repartitionValue=mapIndexValue.repartition(8)
    println("repartition后partiton的个数"+repartitionValue.partitions.length)
    val mapIndexValue1=repartitionValue.mapPartitionsWithIndex((index,y)=>{
      val res=for(e<-y) yield index+":"+e
      res
    })
    mapIndexValue1.foreach(x=>{println(x)})
  }
}
