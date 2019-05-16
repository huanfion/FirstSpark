package com.huanfion.Spark.core

import breeze.linalg.max
import com.huanfion.Spark.core.AggregateByKey.getsc
import org.apache.spark.{SparkConf, SparkContext}

object flodByKey {

  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKey")
    val sc = new SparkContext(sparkconf)
    sc
  }
  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd =sc.parallelize(List((1,1),(1,2),(2,1),(2,3),(2,4),(1,7)),2)

    val mappartionrdd=rdd.mapPartitionsWithIndex((index,iter)=>{
      val res=for (e<-iter) yield index+"_"+e
      res
    })
    mappartionrdd.foreach(x=>println(x))
    var aggregaterdd=rdd.foldByKey(0)((x,y)=>x+y)
    aggregaterdd.foreach(x=>println(x._1+":"+x._2))
    sc.stop()
  }
}
