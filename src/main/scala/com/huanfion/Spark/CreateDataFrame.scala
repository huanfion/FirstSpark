package com.huanfion.Spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.reflect.internal.util.TableDef.Column

object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val customerData=List(("Alex","浙江",39,230.00),("Bob","北京",18,170.00),("Chris","江苏",45,529.95),("Dave","北京",25,99.99),("Ellie","浙江",23,1299.95),("Fred","北京",21,1099.00))
    val spark=SparkSession.builder().config(new SparkConf().setAppName("DataFrame").setMaster("local[2]"))
      .enableHiveSupport().getOrCreate()

    val schema=StructType(Array(
      StructField("customer",StringType,true),
      StructField("province",StringType,true),
      StructField("age",IntegerType,true),
      StructField("total",DoubleType,true)
    ))
    //val customerDF=spark.createDataFrame(customerData);
    val customerDF=spark.createDataFrame(customerData).toDF("customer","province","age","total")
//      .withColumnRenamed("_1","customer")
//      .withColumnRenamed("_2","province")
//      .withColumnRenamed("_3","age")
//      .withColumnRenamed("_4","total")
    import org.apache.spark.sql.functions._
    customerDF.select("customer")
    customerDF.select(col("customer"),col("province"))
    customerDF.orderBy("province")
    customerDF.orderBy(col("province"),col("age"))
    customerDF("province")
  }
}
