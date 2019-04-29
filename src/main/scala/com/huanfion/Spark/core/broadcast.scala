package com.huanfion.Spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量、累加器的使用
 */
object broadcast {

  /*
  通用获取SparkContext对象
   */
  def getsc(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("broadcast").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val list = Array(1, 2, 3, 4, 5)
    val sc = getsc()

    val broadvalue = sc.broadcast(10)
    val rdd = sc.parallelize(list)
    val values = rdd.map(_ * broadvalue.value)
    values.foreach(println(_))
    println(s"-"*20)
    /**
      * 在driver端可以修改广播变量的值，在executor端无法修改广播变量的值
      */
    var count = 0
    var result = rdd.map(x => {
      count += 1
      x
    })
    result.collect()
    println("不使用累加器的结果：")
    println("count is:" + count)


    /**
      * executor修改了变量，根本不会让driver端跟着修改，这个就是累加器出现的原因
      * 累加器提供了将工作节点的值聚合到驱动器程序中
      */
    var accumulator=sc.longAccumulator("count")
    accumulator
    result = rdd.map(x => {
      accumulator.add(1L)
      x
    })
    result.collect()
    println("使用累加器的结果：")
    println("accumulator is:" + accumulator.value)
    sc.stop()
  }
}
