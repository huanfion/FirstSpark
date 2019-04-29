package com.huanfion.Spark.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadSave").master("local[*]")
      //.config("spark.sql.warehourse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse")
      .getOrCreate()
    val userdf = spark.read.load("hdfs://master:9000/data/users.parquet")
    userdf.show()
//    userdf.select("name", "favorite_color").write.mode(SaveMode.Overwrite)
//      .save("hdfs://master:9000/data/namesAndFavColors.parquet")
//    val namecolordf = spark.read.load("hdfs://master:9000/data/namesAndFavColors.parquet")
//    namecolordf.show()
//
//    //读取文件，手动选择保存方式
//    val peopleDF = spark.read.json("hdfs://master:9000/data/people.json")
//    peopleDF.select("name", "age").write.mode(SaveMode.Overwrite).format("parquet").save("hdfs://master:9000/data/nameage.parquet")
//    //或者
//    peopleDF.select("name", "age").write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/data/nameage.parquet")
//    val namagedf = spark.read.format("parquet").load("hdfs://master:9000/data/nameage.parquet")
//    namagedf.show()
  }

}
