package com.huanfion.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLContext {
  def main(args: Array[String]): Unit = {
    var path=args(0)
    //1)创建相应的Context
    var sparkConf = new SparkConf().setAppName("SqlContextApp");
    var sc = new SparkContext(sparkConf)
    var sqlContext = new SQLContext(sc)
    //2)相关的处理
    val people = sqlContext.read.json(path)
    people.printSchema()
    people.show()
    //3)关闭资源
    sc.stop()
  }
}
