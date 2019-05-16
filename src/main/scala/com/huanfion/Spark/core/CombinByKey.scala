package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * (  createCombiner: V => C,  mergeValue: (C, V) => C,  mergeCombiners: (C, C) => C)
  * 对相同K，把V合并成一个集合
  * createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，
  * 要么就 和之前的某个元素的键相同。如果这是一个新的元素,
  * combineByKey() 会使用一个叫作 createCombiner() 的函数来创建个键对应的累加器的初始值
  * mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 方法将该键的累加器对应的当前值与这个新的值进行合并
  * mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。
  * 如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并
  */
object CombinByKey {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(sparkconf)
    sc
  }
  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd=sc.parallelize( Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98)))
    val combinrdd=rdd.combineByKey(
      (v)=>(v,1),
      (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)
    )
    val result=combinrdd.map{
      case (key,value)=>(key,value._1/value._2.toDouble)
    }
    result.foreach(println)
  }
}
