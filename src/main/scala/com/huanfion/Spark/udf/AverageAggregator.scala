package com.huanfion.Spark.udf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


/**
  * 继承Aggregator 相对于UserDefinedAggregateFunction,它的优点是可以带类型
  */
case class User(name: String, age: Long)

case class Average(var sum: Long, var count: Long)

object AverageAggregator extends Aggregator[User, Average, Double] {
  //初始化buffer
  override def zero: Average = Average(0L, 0L)

  //处理一条新记录
  override def reduce(b: Average, a: User): Average = {
    b.sum += a.age
    b.count += 1
    b
  }

  //合并聚合buffer
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //转换reduce的输出类型
  override def finish(reduction: Average): Double = reduction.sum / reduction.count

  //为中间值类型指定编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //为输出类型指定编码器。
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDAFDemo").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    import spark.implicits._
    val userDS = spark.createDataFrame(userData).toDF("name", "age").as[User]
    userDS.show()
    userDS.select(AverageAggregator.toColumn.name("avg")).show()
  }
}
