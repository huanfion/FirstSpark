package com.huanfion.Spark.udf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDAFDemo").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    userDF.createOrReplaceTempView("user_test")
    spark.udf.register("u_avg",AverageUserDefinedAggregateFunction)
    spark.sql("select count(1)  as count,u_avg(age) as avg_age from user_test").show()
  }
}
