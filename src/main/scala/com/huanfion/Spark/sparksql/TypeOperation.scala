package com.huanfion.Spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TypeOperation {

  case class Person(name: String, age: Long)
  case class Employee(name:String,salary:Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrame").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/Work/BigData/FirstSpark/spark-warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    coalesceAndRepartition(spark)
    //    distinct(spark)
    //    filter(spark)
//    mapAndFlatMap(spark)
//    joinWith(spark)
    sample(spark)
  }

  /**
    * 重分区算子
    * coalesce和repartition
    * 区别和rdd的coalesce、repartition算子区别一样
    */
  def coalesceAndRepartition(spark: SparkSession) = {
    val df = spark.read.json("hdfs://master:9000/data/people.json")

    println(df.rdd.partitions.size)
    val personDFRepartition = df.repartition(3)
    println(personDFRepartition.rdd.partitions.size)
    val personDFCoalesce = personDFRepartition.coalesce(2)
    println(personDFCoalesce.rdd.partitions.size)
  }

  /**
    * distinct：是根据每一条数据进行完整内容的比对去重 ，这个方法等同于DropDuplicate不传入任何参数。
    * DropDuplicate：可以根据指定的字段进行去重。
    *
    * @param spark
    */
  def distinct(spark: SparkSession) = {
    val df = spark.read.json("hdfs://master:9000/data/people1.json")
    df.show()
    println("distict去重后结果：")
    df.distinct().show()
    println("dropduplicate对某个字段（name）去重")
    df.dropDuplicates(Seq("name")).show()
  }

  /**
    * filter:根据业务逻辑，如果返回true，就保留该元素，如果是返回false，就不保留
    * except：获取当前df中有，另一个df中没有的
    * intersect:交集
    *
    * @param spark
    */
  def filter(spark: SparkSession) = {
    val df = spark.read.json("hdfs://master:9000/data/people.json")
    df.filter(x => x.getAs[String]("age") != null).show()
    val df1 = spark.read.json("hdfs://master:9000/data/people1.json")
    println("df结果：")
    df.show()
    println("df1结果：")
    df1.show()
    println("df1.except(df)结果：")
    df1.except(df).show()
    println("（交集）df1.intersect(df)结果：")
    df1.intersect(df).show()
  }

  /**
    * map:将数据集中每一条数据都做一次映射，返回一条数据
    * flatmap：将数据集中每条数据都可以返回多条数据
    * mappartition：一次性对一个partition中的数据进行处理。
    * @param spark
    */
  def mapAndFlatMap(spark: SparkSession) = {
    import spark.implicits._
    val ds = spark.read.json("hdfs://master:9000/data/people.json").as[Person]
    ds.show()
    ds.filter("age is not null").map(x => (x.name, x.age + 10))
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "newage").show()

    ds.filter("age is not null").flatMap(x => {
      Seq(Person(x.name + "_1", x.age + 20), Person(x.name + "_2", x.age + 20))
    }).show()

    ds.filter("age is not null").mapPartitions(x => {
      val result = for (elem <- x)
        yield Person(elem.name, elem.age)
      result
    }).show()
  }

  def joinWith(spark:SparkSession)={
    import  spark.implicits._
    val personDS=spark.read.json("hdfs://master:9000/data/people.json").as[Person]
    val employeeDS=spark.read.json("hdfs://master:9000/data/employees.json").as[Employee]
    personDS.show()
    employeeDS.sort($"salary".desc).show()
    employeeDS.sort("salary").show()
    println("joinwith结果：")
    personDS.joinWith(employeeDS,employeeDS("name")===personDS("name")).show()
    println("join结果")
    personDS.join(employeeDS,employeeDS("name")===personDS("name")).show()
  }

  /**
    * randomSplit:根据传入的Array数量确定切分的数量，根据Array的值确定没份数据的权重
    * sample：根据设定的比例随机抽取
    * @param spark
    */
  def sample(spark:SparkSession)={
    import spark.implicits._
    val employeeDS=spark.read.json("hdfs://master:9000/data/employees.json").as[Employee]
//    val orderDF=spark.sql("select * from badou.orders")
//    println("OrderDF count"+orderDF.count())
//    orderDF.randomSplit(Array(2,1,1)).foreach(ds=>println(ds.count()))
//    println("randomSplit切分成3分，没份权重2,1,1：")
//    employeeDS.randomSplit(Array(2,1,1)).foreach(ds=>ds.show())
    println("Sample 按比例随机抽取：")
    employeeDS.sample(false,0.5).show()
  }
}
