package com.huanfion.Spark.project

import org.apache.spark.sql.SparkSession

/*
*DataFrame的其他操作，通过一个简单的案例来展示
 */
object DataFrameCase {

  case class Student(id: Int, name: String, phone: String, email: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Case").master("local[*]").getOrCreate()
    //RDD=>DF
    val rdd = spark.sparkContext.textFile("file:///D:\\BaiduNetdiskDownload\\student.data")
    import spark.implicits._
    val df = rdd.map(x => x.split('|')).map(x => Student(x(0).toInt, x(1), x(2), x(3))).toDF
    //    df.show(30)
    //    df.select("name","email").show(30,false)
    //过滤name为null、和空的记录
    df.filter("name='' OR name='NULL'").show()
    df.filter("substring(name,0,1)='M'").show()
    import org.apache.spark.sql.functions._
    df.sort(col("name"),col("id").desc).show()
    spark.stop()
  }
}
