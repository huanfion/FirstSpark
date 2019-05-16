package com.huanfion.Spark.core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object partitionBy {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Distinct")
    val sc = new SparkContext(sparkconf)
    sc
  }
  def main(args: Array[String]): Unit = {

    val sc=getsc()
    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(rdd.partitions.size)
    val partitonrdd=rdd.partitionBy(new HashPartitioner(3))
    partitonrdd.mapPartitionsWithIndex((index:Int,iter:Iterator[(Int,String)])=>{
      val res=for(e<-iter) yield e._1+"_"+e._2+":"+index
      res
    }).foreach(println)
    println(partitonrdd.partitions.size)
  }
}
