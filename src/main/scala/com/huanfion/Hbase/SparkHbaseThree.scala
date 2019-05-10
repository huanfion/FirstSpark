package com.huanfion.Hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
* 使用SaveAsNewAPIHadoopDataset写入数据
* */
object SparkHbaseThree {
  def main(args: Array[String]): Unit = {
    // HBase zookeeper
    val ZOOK_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOK_HOSTIP.map(_ + ":2181").mkString(",")

    val sparkConf=new SparkConf().setAppName("SparkHbaseThree")
    val spark = SparkSession.builder().appName("spark to hbase")
      .master("local[*]").config("hive.metastore.uris", "thrift://master:9083")
      .enableHiveSupport().getOrCreate()
    val sc=spark.sparkContext;

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"orders3")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string
    //这个地址还必须是不存在的，否则报错Output directory /tmp already exists
    sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.outputdir","/tmp/SparkHbaseThree")

    val job=Job.getInstance(sc.hadoopConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val rdd=spark.sql("select order_id,user_id,order_dow from badou.orders limit 300").rdd
    rdd.map(row=>{
      val order_id = row(0).asInstanceOf[String]
      val user_id = row(1).asInstanceOf[String]
      val order_dow = row(2).asInstanceOf[String]
      val family=Bytes.toBytes("id")
      val ordercol=Bytes.toBytes("order")
      val dowcol=Bytes.toBytes("dow")
      /**
        * 一个put对象就是一行记录，在构造方法中指定主键user_id
        * 所有的插入数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
        */
      val put = new Put(Bytes.toBytes(user_id))
      put.addImmutable(family, ordercol, Bytes.toBytes(order_id))
      put.addImmutable(family,dowcol, Bytes.toBytes(order_dow))
      (new ImmutableBytesWritable,put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
