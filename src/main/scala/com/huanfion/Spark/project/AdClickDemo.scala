package com.huanfion.Spark.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

/**
  * spark rdd的操作实现简单的统计操作
  * 1.统计每一个省份点击TOP3的广告ID    省份+广告
  * 2.统计每一个省份每一个小时的TOP3广告的ID
  */
object AdClickDemo {
  def getsc(): SparkContext = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("AdClick")
    val sc = new SparkContext(sparkconf)
    sc
  }

  case class AdClick(timestamp: Long, province: Int, city: Int, userid: Int, adid: Int)

  /*
    统计每一个省份点击TOP3的广告ID    省份+广告
   */
  def getProvinceTop3(sc: SparkContext) = {
    val logRDD: RDD[String] = sc.textFile("hdfs:///data/agent.log")
    //统计每一个省份的广告总量
    val proandad2Sum = logRDD.map { x =>
      //将数据转化为对象
      val param = x.split(" ")
      AdClick(param(0).toLong, param(1).toInt, param(2).toInt, param(3).toInt, param(4).toInt)
    }.map(adClick => (adClick.province + "_" + adClick.adid, 1))
      .reduceByKey(_ + _)
    val pro2Ads = proandad2Sum.map(x => {
      val param = x._1.split("_")
      // 省份 + （广告id，点击数）
      (param(0), (param(1).toInt, x._2))
    }).groupByKey()
    val protop3 = pro2Ads.flatMap { case (pro, items) => {
      val filterItems: List[(Int, Int)] = items.toList.sortWith((x, y) => x._2 > y._2).take(3)

      val result = new ArrayBuffer[String]()
      for (item <- filterItems) {
        result += pro + "、" + item._1 + "、" + item._2
      }
      result
    }
    }
    protop3.collect().foreach(println)
  }

  /*
  统计每一个省份每一个小时的TOP3广告的ID
   */
  def getProvinceHourTop3(sc: SparkContext) = {
    val logRDD: RDD[String] = sc.textFile("hdfs:///data/agent.log")
    //返回 每个省份每个小时的广告id对应的点击数
    val prohour2sum = logRDD.map { x =>
      val param = x.split(" ")
      AdClick(param(0).toLong, param(1).toInt, param(2).toInt, param(3).toInt, param(4).toInt)
    }.map(x => {
      (x.province + "_" + getHour(x.timestamp) + "_" + x.adid, 1)
    }).reduceByKey(_ + _)
    val prohour2Ads = prohour2sum.map { x =>
      val param = x._1.split("_")
      //省份-小时，（广告id，点击总数）
      (param(0) + "_" + param(1), (param(2).toInt, x._2))
    }.groupByKey()
    val prohourtop3 = prohour2Ads.flatMap {
      case (prohour, items) => {
        val filterItems = items.toList.sortWith(_._2 > _._2).take(3)
        val result = new ArrayBuffer[String]()
        for (item <- filterItems) {
          result += prohour + "、" + item._1 + "、" + item._2
        }
        result
      }
    }
    prohour2sum.collect().foreach(println)
  }

  /*
 根据timestamp 获取当前小时
   */
  def getHour(timestamp: Long) = {
    val dateTime = new DateTime(timestamp)
    dateTime.getHourOfDay.toString
  }

  def main(args: Array[String]): Unit = {
    val sc = getsc();
    //1.统计每一个省份点击TOP3的广告ID    省份+广告
    getProvinceTop3(sc)
    //2.统计每一个省份每一个小时的TOP3广告的ID
    getProvinceHourTop3(sc)
    sc.stop()
  }
}
