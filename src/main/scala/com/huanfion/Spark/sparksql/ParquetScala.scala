package com.huanfion.Spark.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Parquet数据的处理
  */
object ParquetScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetScala").master("local[*]")
      .config("spark.sql.warehourse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse")
      .getOrCreate()
    basicLoad(spark)
//    merge(spark)
  }

  def basicLoad(spark:SparkSession)={
    //读取文件，手动选择保存方式
    val peopleDF = spark.read.json("hdfs://master:9000/data/people.json")
    peopleDF.write.mode(SaveMode.Overwrite).format("parquet").save("hdfs://master:9000/data/people.parquet")
    //或者
    //peopleDF.select("name", "age").write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/data/nameage.parquet")
    spark.read.parquet("hdfs://master:9000/data/people.parquet").createOrReplaceTempView("people")
    spark.sql("select * from people").show()
  }
  def merge(spark:SparkSession)={
    import  spark.implicits._
    val squareDF=spark.sparkContext.makeRDD(1 to 5).map(x=>(x,x*x)).toDF("value","square")
    squareDF.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/data/test_table/key=1")

    val cubeDF=spark.sparkContext.makeRDD(6 to 10).map(x=>(x,x*x*x)).toDF("value","cube")
    cubeDF.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/data/test_table/key=2")

    val mergeDF=spark.read.option("mergeSchema","true").parquet("hdfs://master:9000/data/test_table")
    mergeDF.printSchema()
    mergeDF.show()
  }
}
