package com.huanfion.Spark.core

import com.huanfion.Spark.core.Coalesce.getsc
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 随机抽取算子
  */
object TaskSample {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(Array("hello","jason","jim","vin","test","mary"))
    println("withReplacement=false,父本个数"+rdd.count()+",样本个数：3")
    val takesamplerdd=rdd.takeSample(false,3)
    println("抽取样本结果：")
    takesamplerdd.foreach(x=>println(x))

    println("withReplacement=false,父本个数"+rdd.count()+",样本个数：8")
    val takesamplerdd1=rdd.takeSample(false,8)
    println("抽取样本结果：")
    takesamplerdd1.foreach(x=>println(x))

    println("withReplacement=true,父本个数"+rdd.count()+",样本个数：3")
    val takesamplerdd2=rdd.takeSample(true,3)
    println("抽取样本结果：")
    takesamplerdd2.foreach(x=>println(x))

    println("withReplacement=true,父本个数"+rdd.count()+",样本个数：8")
    val takesamplerdd3=rdd.takeSample(true,8)
    println("抽取样本结果：")
    takesamplerdd3.foreach(x=>println(x))
  }
}
