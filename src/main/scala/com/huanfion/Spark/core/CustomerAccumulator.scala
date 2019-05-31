package com.huanfion.Spark.core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

class CoumterAcc extends AccumulatorV2[String, HashMap[String, Int]] {
  private var _hashacc = new HashMap[String, Int]

  //检查是否为空
  override def isZero: Boolean = {
    _hashacc.isEmpty
  }

  //拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, HashMap[String, Int]] = {
    val newAcc = new CoumterAcc()
    _hashacc.synchronized {
      newAcc._hashacc ++= _hashacc
    }
    newAcc
  }

  //重置累加器
  override def reset(): Unit = {
    _hashacc.clear()
  }

  //每个分区中用于添加数据的方法
  override def add(v: String): Unit = {
    _hashacc.get(v) match {
      case None => _hashacc += ((v, 1))
      case Some(a) => _hashacc += ((v, a + 1))
    }
  }

  //合并每个分区中的输出
  override def merge(other: AccumulatorV2[String, HashMap[String, Int]]): Unit = {

    other match {
      case  o:AccumulatorV2[String, HashMap[String, Int]]=>{
        for ((k, v) <- o.value) {
          _hashacc.get(k) match {
            case None => _hashacc += ((k, v + 1))
            case Some(a) => _hashacc += ((k, v + a))
          }
        }
      }
    }

  }
//输出
  override def value: HashMap[String, Int] = {
    _hashacc
  }
}

object CustomerAccumulator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("CustomerAcc").setMaster("local[*]"))
    val rdd = sc.makeRDD(Seq("a", "b", "c", "a", "d", "c", "b", "d", "b"))

    //声明自定义累加器
    val customerAcc=new CoumterAcc()
    sc.register(customerAcc,"acc")

    rdd.foreach(x=>{
      customerAcc.add(x)
    })
    for((k,v)<-customerAcc.value){
      println("【"+k+":"+v+"】")
    }
    sc.stop()
  }
}
