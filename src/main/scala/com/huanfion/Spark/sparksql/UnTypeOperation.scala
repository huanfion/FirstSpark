package com.huanfion.Spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UnTypeOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrame").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val df = spark.read.json("hdfs://master:9000/data/people.json")
    //df.where()
    //    df.select()
    //    df.selectExpr()
    //    df.groupBy().sum().orderBy()
    //    df.createOrReplaceTempView()
    val orderDF = spark.sql("select * from badou.orders")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    orderDF.select(current_date(), current_timestamp(), rand(), round($"days_since_prior_order", 2), concat($"user_id", $"order_id"))

  }
}
