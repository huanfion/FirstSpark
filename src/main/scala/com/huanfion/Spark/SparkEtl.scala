package com.huanfion.Spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkEtl {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.2.0-bin-master")
    val conf=new SparkConf().setAppName("spark 清洗").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf)
      .getOrCreate()
    val sc=spark.sparkContext
    val rdd=sc.textFile("file:///D:\\Work\\BigData\\FirstSpark\\src\\main\\scala\\com\\huanfion\\Spark\\data")
//spark.createDataF
    rdd.foreach(println(_))
  }
}
