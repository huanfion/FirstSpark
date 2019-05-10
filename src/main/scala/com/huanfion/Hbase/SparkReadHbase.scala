package com.huanfion.Hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    val ZOOKEEPER_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOKEEPER_HOSTIP.map(_ + ":2181").mkString(",")

    val spark = SparkSession.builder().appName("ReadHbase")
      .master("local[*]")
      .enableHiveSupport().getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "orders")

    /*val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable("orders")) {
      val tableDesc = new HTableDescriptor(TableName.valueOf("orders"))
      admin.createTable(tableDesc)
    }*/

    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val count = hbaseRDD.count()
    print("hbaseRDD.count:" + count)

    hbaseRDD.foreach {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val order_id = Bytes.toString(result.getValue("id".getBytes(), "order_id".getBytes()))
        val dow = Bytes.toString(result.getValue("id".getBytes(), "dow".getBytes()))
        println("RowKey:" + key + "orderid:" + order_id + "order_dow:" + dow)
      }
    }
    spark.stop()
  }
}
