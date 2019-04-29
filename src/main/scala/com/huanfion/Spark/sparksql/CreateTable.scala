package com.huanfion.Spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CreateTable {
  def getspark(): SparkSession = {
    val spark = SparkSession.builder().appName("CreateTable").master("local[*]").enableHiveSupport().getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = getspark()
    val sql=
      """
        |create table if not exists qq_user(
        |Im_date bigint,
        |qq bigint,
        |age string,
        |sex string,
        |area string
        |)
      """.stripMargin
    val sql2="insert into qq_user values(20170101,1000,'20','女','广东省')"
    spark.sql(sql)
    spark.sql(sql2)
    spark.stop()
  }
}
