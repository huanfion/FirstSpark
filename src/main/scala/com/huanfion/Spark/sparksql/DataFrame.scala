package com.huanfion.Spark.sparksql

import org.apache.spark.sql.SparkSession

/**
  * 创建DataFrame
  */
object DataFrame {
  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    /**
      * Create DataFrame
      */
    val path = "hdfs://master:9000/data/people.json"
    val spark = SparkSession.builder().appName("DataFrame").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").getOrCreate()

    import spark.implicits._
    var df = spark.read.format("json").load(path)
    df.show()
    //val df = spark.read.json(path)
    df.show()
    df.printSchema()
    //import org.apache.spark.sql.functions._
    //df.select(col("name")).show()
    df.select($"name", $"age" + 1).show()

    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    //三种方式创建DataSet
    //import spark.implicits._
    val caseClassDS = Seq(Person("cherry", 23)).toDS()
    caseClassDS.show()

    val commonDS=Seq(1,2,3).toDS()
    commonDS.map(x=>x+1).show()
    commonDS.show()

    val jsonDS=spark.read.json("hdfs://master:9000/data/people.json").as[Person]
    jsonDS.show()
    spark.stop()

    //RDD 转DataFrame
  }
}
