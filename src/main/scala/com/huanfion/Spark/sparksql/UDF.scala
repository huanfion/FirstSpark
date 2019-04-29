package com.huanfion.Spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDF").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    userDF.show()
    userDF.createOrReplaceTempView("user_test")
    //1.通过匿名函数注册udf
    spark.udf.register("strlen", (x: String) => {
      x.length
    })
    spark.sql("select strlen(name),name,age from user_test").show()
    //2.通过实名函数注册udf 实名函数的注册有点不同，要在后面加 _(注意前面有个空格)
    def is_Adult(age:Int)={
      if(age>18) true else  false
    }
    spark.udf.register("isAdult",is_Adult _)
    spark.sql("select isAdult(age),name,age from user_test").show()
    /*
    *3.DataFrame的udf方法，虽然名字一样，但是属于不同类，它在
    * org.apache.spark.sql.functions里
     */
    import org.apache.spark.sql.functions._
    val strLen=udf((x:String)=>{x.length})
    val udf_isAdult=udf(is_Adult _)
    //通过withColumn添加列
    import spark.implicits._
    userDF.withColumn("name_len",strLen($"name"))
      .withColumn("isAdult",udf_isAdult($"age")).show()
    //通过select 添加列
    userDF.select($"*",strLen($"name") as "name_len",udf_isAdult($"age") as "is_Adult").show()

    spark.stop()
  }
}
