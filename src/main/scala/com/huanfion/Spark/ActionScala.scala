package com.huanfion.Spark

import org.apache.spark.{SparkConf, SparkContext}

object ActionScala {
  def getsc: SparkContext = {
    val sparkconf = new SparkConf().setAppName("action").setMaster("local")
    new SparkContext(sparkconf)
  }

  def main(args: Array[String]): Unit = {
    //    reduce
    //    count
    //    collect
//    take
    countByKey
  }

  def reduce = {
    val list = Array(1, 2, 3, 4)
    val rdd = getsc.parallelize(list)
    System.out.println(rdd.reduce(_ + _))
  }

  def count = {
    val list = Array(1, 2, 3, 4)
    val rdd = getsc.parallelize(list)
    System.out.println(rdd.count())
  }

  def collect = {
    val list = Array(1, 2, 3, 4)
    val rdd = getsc.parallelize(list)
    val value = rdd.collect();
    value.foreach(x => System.out.println(x))
  }

  def take = {
    val list = Array(1, 2, 3, 4)
    val rdd = getsc.parallelize(list)
    val value=rdd.take(2)
    value.foreach(x=>System.out.println(x))
  }
  def countByKey={
    val list = Array(new Tuple2("class_1", 91),
      Tuple2("class_2", 78),
      Tuple2("class_1", 99),
      Tuple2("class_2", 76),
      Tuple2("class_2", 90));
    val rdd=getsc.parallelize(list)
    val countvalue=rdd.countByKey()
    countvalue.foreach(x=>System.out.println(x._1+":"+x._2))
  }
}
