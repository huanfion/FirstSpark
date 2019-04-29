package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * TopN实践
  */
object TopN {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc()
    val rdd = sc.parallelize(List(23, 12, 56, 44, 23, 99, 13, 57))
    val beginSort = rdd.map(x => (x, x))
    val sortValue = beginSort.sortByKey(false)

    val beginTopN = sortValue.map(x => x._2)
    val topN = beginTopN.take(3)
    for (elem <- topN) {
      println(elem)
    }
  }
}
