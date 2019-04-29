package com.huanfion.Spark.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object RDDToDFProgram {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDDToDFProgram").master("local[*]")
      .config("spark.sql.warehourse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse")
      .getOrCreate()

    val peopleRDD=spark.sparkContext.textFile("hdfs://master:9000/data/people.txt")
    val fileds=Array(new StructField("name",StringType,false), new StructField("age",LongType,true))
    val schema=new StructType(fileds)
    val rowRDD=peopleRDD.map(x=>x.split(",")).map(x=>Row(x(0),x(1).trim.toLong))

    val df=spark.createDataFrame(rowRDD,schema)
    df.createOrReplaceTempView("person")
    val resultDF=spark.sql("select * from person where age>20")
    resultDF.show()
  }
}
