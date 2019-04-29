package com.huanfion.Spark.sparksql

import org.apache.spark.sql.SparkSession

object HiveSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveSpark").master("local[*]")
      .config("hive.metastore.uris", "thrift://master:9083")
      .enableHiveSupport()
      .getOrCreate()
    val orderdf = spark.sql("select * from default.qq_user")
    orderdf.show()
  }
}
