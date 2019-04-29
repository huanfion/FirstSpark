package com.huanfion.Spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object LogicalPlan {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogicalPlan").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val stu = "stu"
    import spark.implicits._
    Seq((0,"xiaozhang",10),
      (1,"xiaohong",11),
      (2,"xiaoli",12)).toDF("id","name","age").createOrReplaceTempView(stu)
    val score="score"
    Seq((0,"语文",80),(0,"数学",100),(0,"英语",99),
      (1,"语文",40),(1,"数学",50),(1,"英语",60),
      (2,"语文",70),(2,"数学",80),(2,"英语",90)).toDF("id","course","score").createOrReplaceTempView(score)
    val queryExecution=spark.sql("select sum(v),name from "+
    "(select stu.id,100+10+score.score as v,name from stu join score where stu.id=score.id and stu.age>=11) tmp "+
      "group by name").queryExecution

  }
}
