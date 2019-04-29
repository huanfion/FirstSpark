package com.huanfion.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationScala {
  def getsc():SparkContext={
    val sparkconf=new SparkConf().setAppName("Transformation").setMaster("local")

    val sc = new SparkContext(sparkconf)

    sc
  }
  def main(args: Array[String]): Unit = {
//    filter()
    //flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
    cogroup()
  }

  def filter():Unit={
   val sc=getsc()

    val list= Array(1,2,3,4,5,6,7,8,9,10)

    val rdd=sc.parallelize(list)

    val count=rdd.filter(x=>x%2==0)

    count.foreach(x=>System.out.println(x))
  }
  def map():Unit={
    val sparkconf=new SparkConf().setAppName("Transformation").setMaster("local")

    val sc = new SparkContext(sparkconf)

    val list= Array(1,2,3,4,5)

    val rdd=sc.parallelize(list)

    val count=rdd.map(x=>x*10)

    count.foreach(x=>System.out.println(x))
  }

  def flatMap():Unit={
    val list=Array("Hadoop Hive", "Hadoop Hbase")

    val rdd=getsc().parallelize(list)

    val flatmapvalue=rdd.flatMap(x=>x.split(" "))

    flatmapvalue.foreach(x=>System.out.println(x))
  }

  def groupByKey()= {
    val list=Array(
        Tuple2("class_1",90),
       Tuple2("class_2",78),
       Tuple2("class_1",99),
       Tuple2("class_2",76),
       Tuple2("class_2",90),
       Tuple2("class_1",86))
    val rdd=getsc().parallelize(list)

    val groupvalue=rdd.groupByKey()

    groupvalue.foreach(x=>{
      System.out.println(x._1)
      x._2.foreach(y=>System.out.println(y))
    })
  }

  def reduceByKey()={
    val list=Array(
      Tuple2("class_1",90),
      Tuple2("class_2",78),
      Tuple2("class_1",99),
      Tuple2("class_2",76),
      Tuple2("class_2",90),
      Tuple2("class_1",86))
    val rdd=getsc().parallelize(list)

    val reducevalue=rdd.reduceByKey(_+_)

    reducevalue.foreach(x=>System.out.println(x._1+"--"+x._2))
  }

  def sortByKey()={
    val list=Array(
      Tuple2("liuda",90),
      Tuple2("lier",78),
      Tuple2("zhangsan",99),
      Tuple2("gousi",76),
      Tuple2("lily",90),
      Tuple2("lucy",86))
    val rdd=getsc().parallelize(list)
    //rdd.map(x=>(x._2,x._1)).sortByKey()
    val sortvalue=rdd.sortBy(x=>x._2,false)

    sortvalue.foreach(x=>System.out.println(x._1+":"+x._2))
  }
  def join()={
    val stulist=Array(
       Tuple2(1,"liuda"),
       Tuple2(2,"lier"),
       Tuple2(3,"zhangsan"),
       Tuple2(4,"gousi"),
       Tuple2(5,"lily"),
       Tuple2(6,"lucy"));
    val scorelist=Array(
       Tuple2(1,88),
       Tuple2(2,87),
       Tuple2(3,90),
       Tuple2(4,100),
       Tuple2(5,58),
       Tuple2(6,65));
    val sc=getsc();
    val sturdd=sc.parallelize(stulist)

    val scorerdd=sc.parallelize(scorelist)

    val joinvalue=sturdd.join(scorerdd)

    joinvalue.foreach(x=>System.out.println(x._1+"->"+x._2))
  }

  def cogroup()={
    val stulist=Array(
      Tuple2(1,"liuda"),
      Tuple2(2,"lier"),
      Tuple2(3,"zhangsan"),
      Tuple2(4,"gousi"),
      Tuple2(5,"lily"),
      Tuple2(6,"lucy"));
    val scorelist=Array(
      Tuple2(1,88),
      Tuple2(2,87),
      Tuple2(2,84),
      Tuple2(2,86),
      Tuple2(3,90),
      Tuple2(3,65),
      Tuple2(4,100),
      Tuple2(5,58),
      Tuple2(6,65));
    val sc=getsc();
    val sturdd=sc.parallelize(stulist)

    val scorerdd=sc.parallelize(scorelist)

    val joinvalue=sturdd.cogroup(scorerdd)

    joinvalue.foreach(x=>System.out.println(x._1+"->"+x._2._1.toList+":"+x._2._2.toList))
  }
}
