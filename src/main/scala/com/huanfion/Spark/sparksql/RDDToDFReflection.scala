package com.huanfion.Spark.sparksql

import org.apache.spark.sql.SparkSession

object RDDToDFReflection {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDDToDFReflection").master("local[*]")
      .config("spark.sql.warehourse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse")
      .getOrCreate()
    val path = "hdfs://master:9000/data/people.txt"
    import spark.implicits._
    val df = spark.sparkContext.textFile(path).map(x => x.split(",")).map(x => {
      new Person(x(0), x(1).trim.toLong)
    }).toDF()
    df.show()

    df.createOrReplaceTempView("person")
    val resultDF=spark.sql("select * from person a where a.age>20")

    val resultRDD=resultDF.rdd.map(row=>{
      val name=row.getAs[String]("name")
      val age=row.getAs[Long]("age")
      Person(name,age)
    })
    for (elem <- resultRDD.collect()) {
      println(elem)
    }
  }
}
