package com.huanfion.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object ScanHBase {
  /*def main(args: Array[String]): Unit = {
    val ZOOK_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOK_HOSTIP.map(_ + ":2181").mkString(",")
    //System.setProperty("spark.serializer", "org.apache.serializer.KryoSerializer")
    val tablename="orders"
    val jobConf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    jobConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    jobConf.set("hbase.zookeeper.property.clientPort","2181")
//    jobConf.set(TableInputFormat.INPUT_TABLE,tablename)

    val scan=new Scan()
    val proto=ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray());
    jobConf.set(TableInputFormat.SCAN,ScanToString)
    val spark = SparkSession.builder().appName("ReadHbase").enableHiveSupport().getOrCreate()
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(jobConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.foreach{case(_,result)=>{
      val key=Bytes.toString(result.getRow)
      val order_id=Bytes.toString(result.getValue("id".getBytes(),"order".getBytes()))
      val dow=Bytes.toString(result.getValue("id".getBytes(),"dow".getBytes()))
      println("RowKey:"+key+"orderid:"+order_id+"order_dow:"+dow)
    }};
  }*/
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.TRACE)
    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[*]")
    val ZOOK_HOSTIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZOOKEEPER_QUORUM = ZOOK_HOSTIP.map(_ + ":2181").mkString(",")
    // 创建hbase configuration
    val hBaseConf:Configuration = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"orders")
    hBaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    // 创建 spark context
    val sc = new SparkContext(sparkConf)

    // 从数据源获取数据
    val hbaseRDD:RDD[(ImmutableBytesWritable, Result)]=
      sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    print(hbaseRDD.count())
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val order = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"),Bytes.toBytes("order_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"),Bytes.toBytes("dow")))
    )).toDF("order_id","order_dow")
    order.show()

  }
}
