package com.huanfion.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  */
object SecondSort {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setAppName("SecondSort").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)
    sc
  }

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.2.0-bin-master")
    val sc = getsc()
    //注意这里需要设置分区数，sortByKey是按分区进行排序的，如果此处不设置，sortValues.collect()不可少
    val rdd = sc.textFile("hdfs:///user/huanfion/data/secondSort.txt", 1)
    //val rdd = sc.parallelize(List("class1 67", "class2 89", "class1 78", "class2 90", "class1 99", "class3 34", "class3 89"),1)
    //val rdd=sc.parallelize(List("1 23","3 22","3 31","1 12","2 11","4 45"))
    println(rdd.partitions.length)
    val beginSortValues = rdd.map(x => (new SecondSortKey(x.split(" ")(0),
      x.split(" ")(1).toDouble), x))
    val sortValues = beginSortValues.sortByKey(true)
    println(sortValues.partitions.length)
    sortValues.collect().foreach(x => println(x._2))
    //sortValues.saveAsTextFile("hdfs:///user/huanfion/output/")
    sc.stop()
  }
}

/**
  * 自定义key 继承Ordered
  */
class SecondSortKey(val firstkey: String, val secondkey: Double)
  extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    val comp = this.firstkey.compareTo(that.firstkey)
    if (comp == 0) {
      return this.secondkey.compareTo(that.secondkey)
    }
    return comp
  }
}
