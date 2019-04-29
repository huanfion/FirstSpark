package com.huanfion.Spark

import java.util.Properties

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {

  case class Movie(actor_name: String, movie_title: String, age: Long)

  case class Employee(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf)
      .enableHiveSupport().getOrCreate()

    val lines = spark.sparkContext.textFile("hdfs:///The_Man_of_Property.txt")
    val words = lines.flatMap(line => line.split(" "))
    val word = words.map((_, 1))
    val wordcount = word.reduceByKey((_ + _))
    val sortcount = wordcount.sortBy(_._2, false)
    sortcount.take(10).foreach(x => println(x._1 + ":" + x._2))

/*    val df=spark.sql("select * from userdb.order")
    val pro=new Properties()
    pro.setProperty("driver","com.mysql.jdbc.Driver")
    df.write.jdbc("jdbc:mysql://master:3306/test?user=root&password=123456","sparkorder",pro)

    val jdbcDf=spark.read.format("jdbc")
      .option("url","jdbc:mysql://master:3306/test")
      .option("dbtable","sparkorder")
      .option("user","root")
      .option("password","123456").load()
    val lines=spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
    val jdbcDf2=spark.read.jdbc("jdbc:mysql://master:3306/test?user=root&password=123456","sparkorder",pro)*/
    /*val rdd1=spark.sparkContext.parallelize(Seq(("a",1),("b",1),("a",1)))
    rdd1.map { x =>
      println("运行")
      x._1
    }
//    val df=spark.sql("select * from userdb.order")
//    val rdd=df.select("order_dow").rdd;
//    //查找dow不等于1的记录数
//    val count1=rdd.map(x=>x.getString(0)).filter(_!="1").count();
      val rdd=spark.sparkContext.parallelize(1 to 10).map(x=>(x,x*x))
      val dataframe=spark.createDataFrame(rdd).toDF("key","value")
      dataframe.show()
      import spark.implicits._
    rdd.toDS()
    //创建dataset
    val movies = Seq(Movie("DDLJ", "Awesome", 2018L),  Movie("ADHM", "Nice", 2018L))
    val moviesDS = movies.toDF.as[Movie]
    moviesDS.show()

    val moviesDS1=spark.createDataset(movies)


    val caseClassDS = Seq(Employee("Amy", 32)).toDS
    caseClassDS.show()

    val movies1 = Seq(("Damon, Matt", "The Bourne Ultimatum", 2007L),
      ("Damon, Matt", "Good Will Hunting", 1997L))
    val moviesDF = movies1.map(m=>Movie(m._1,m._2,m._3)).toDF().as[Movie]
    val moviesDF1=movies1.map(m=>Movie.tupled(m)).toDS()
*/
  }
}
