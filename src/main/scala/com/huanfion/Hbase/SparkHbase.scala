package com.huanfion.Hbase

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/*
* Put(List) Hbase
* Spark 写入数据到HBase
* */
object SparkHbase {
  //从hive取数据DataFrame-》RDD写入hbase，道理和从flume中取一样，因为hive取出来的DataFrame转成rdd了。
  def main(args: Array[String]): Unit = {
    //设置zookeeper 集群
    import scala.collection.JavaConverters._
    val ZOOKEEPER_IP = List("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOKEEPER_IP.map(_ + ":2181").mkString(",")
    //    print(ZOOKEEPER_QUORUM)
    //构建SparkSession实例
    val spark = SparkSession.builder()
      .master("local[*]").config("hive.metastore.uris", "thrift://master:9083")
      .appName("SparkPutHbase").enableHiveSupport().getOrCreate()
    try {
      val orderRDD = spark.sql("select user_id,order_id,order_dow from badou.orders limit 300").rdd

      orderRDD.map(x => {
        val user_id = x(0).toString
        val order_id = x(1).toString
        val dow = x(2).toString
        val put = new Put(Bytes.toBytes(user_id))
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("order_id"), Bytes.toBytes(order_id))
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("dow"), Bytes.toBytes(dow))
        put
      })
        .foreachPartition(partition => {
          val hbaseconf = HBaseConfiguration.create()
          hbaseconf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
          hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
          val conn = ConnectionFactory.createConnection(hbaseconf)
          //获取Table
          val table = conn.getTable(TableName.valueOf("orders"))
          //注意，实际使用中partition数据量可能过多，可能会导致内存溢出或者put执行假死情况
          //建议取一定数据量的数据就put一次，比如1w条put一次。
          import scala.collection.JavaConversions._
          table.put(seqAsJavaList(partition.toSeq))
          table.close()
          conn.close()
        })
    }
    catch {
      case e: Exception => println(e.getMessage)
    }
    spark.stop()
  }
}
