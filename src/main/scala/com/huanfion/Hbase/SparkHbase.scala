package com.huanfion.Hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/*
* Put(List) Hbase
* */
object SparkHbase {
  //从hive取数据DataFrame-》RDD写入hbase，道理和从flume中取一样，因为hive取出来的DataFrame转成rdd了。
  def main(args: Array[String]): Unit = {
    // HBase zookeeper
    val ZOOK_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOK_HOSTIP.map(_ + ":2181").mkString(",")

    //    print(ZOOKEEPER_QUORUM)

    val spark = SparkSession.builder().appName("spark to hbase").enableHiveSupport().getOrCreate()
    try {
      val rdd = spark.sql("select order_id,user_id,order_dow from badou.orders limit 300").rdd

      rdd.map { row =>
        val order_id = row(0).asInstanceOf[String]
        val user_id = row(1).asInstanceOf[String]
        val order_dow = row(2).asInstanceOf[String]

        /**
          * 一个put对象就是一行记录，在构造方法中指定主键user_id
          * 所有的插入数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
          */
        val put = new Put(Bytes.toBytes(user_id))
        put.addImmutable(Bytes.toBytes("id"), Bytes.toBytes("order"), Bytes.toBytes(order_id))
        put.addImmutable(Bytes.toBytes("id"), Bytes.toBytes("dow"), Bytes.toBytes(order_dow))
        put
      }.foreachPartition {partition =>
        //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
        val table = createTable(ZOOKEEPER_QUORUM)
        //HTable方法被弃用了
        //val table=new HTable(jobConf,TableName.valueOf("orders"))
        import scala.collection.JavaConversions._
        table.put(seqAsJavaList(partition.toSeq))
        table.close()
      }
    }
    finally {
      spark.stop()
    }

  }

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
