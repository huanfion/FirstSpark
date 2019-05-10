package com.huanfion.Hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Put(List) Hbase
  * * Spark 写入数据到HBase  和SparkHbase一样的，这个自己写来练习的
  */
object SparkHbasePut {
  val spark=SparkSession.builder().appName("HbasePut").master("local[*]")
    .enableHiveSupport().getOrCreate()
  // HBase zookeeper
  val ZOOK_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
  val ZOOKEEPER_QUORUM = ZOOK_HOSTIP.map(_ + ":2181").mkString(",")

  val ages=List(7,8,9)
  val rdd=spark.sparkContext.makeRDD(ages)

  rdd.foreach(t=>{
    print(t)
    val table=createTable(ZOOKEEPER_QUORUM)
    val put=new Put(Bytes.toBytes("rowkey00"+t))
  })
  def createTable(ZOOKEEPER_QUORUM: String): Table = {
    val jobConf = new JobConf(HBaseConfiguration.create())
    jobConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //HTable
    val conn = ConnectionFactory.createConnection(jobConf)
    conn.getTable(TableName.valueOf("orders"))
  }
}
