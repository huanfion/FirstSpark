package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Coalesce 将RDD的partition数量缩减，将一定量的数据压缩到更少放入partition中
  * 一般和filter算子一起使用，使用filter过滤很多数据后，partition中的数据不均匀，
  * 此时我们可以使用coalesce算子压缩RDD的partition数量，从而让partition中数据均匀
  */
object Coalesce {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Coalesce")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(Array("hello","jason","jim","vin","test","mary"),5)
    println("coalesce 前的partition个数"+rdd.partitions.length)
    val mapIndexValue=rdd.mapPartitionsWithIndex((index,y)=>{
      val res=for(e<-y) yield index+":"+e
      res
    })
    mapIndexValue.foreach(x=>{println(x)})

    val coalesceValue=mapIndexValue.coalesce(8,true)
    println("coalesce后partiton的个数"+coalesceValue.partitions.length)
    val mapIndexValue1=coalesceValue.mapPartitionsWithIndex((index,y)=>{
      val res=for(e<-y) yield index+":"+e
      res
    })
    mapIndexValue1.foreach(x=>{println(x)})
  }
}
