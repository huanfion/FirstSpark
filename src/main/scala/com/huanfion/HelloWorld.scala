package com.huanfion

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import Array._

object HelloWorld {

  implicit class SayHelloImp(ivalue: Int) {
    val value = ivalue

    def sayHello = {
      println(s"$ivalue say hello")
    }
  }

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    //    val spark = SparkSession
    //      .builder()
    //      .appName("appName")
    //      .enableHiveSupport().getOrCreate()
    //
    //    val sc = new SparkContext(conf)
    //    val nums = Array(3,2,4)
    //    twoSum(nums, 6).foreach(print(_))

  }
  def twoSum(nums: Array[Int], target: Int): Array[Int] = {
    var cache = scala.collection.immutable.Map[Int, Int]()
    for (i: Int <- range(0, nums.length)) {
      if (cache.contains(target - nums(i))) {
        return Array(cache(target - nums(i)), i)
      } else {
        cache += (nums(i) -> i)
      }
    }
    Array(0, 0)
  }
}
