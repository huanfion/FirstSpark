

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverFromKafkaWC {

  case class Order(order_id: String, user_id: String)

  def main(args: Array[String]): Unit = {
    val Array(group_id, topic, exectime, dt) = Array("group_test","badou","20","20181125")
    val ZkHostIP = Array("128", "130", "132").map("192.168.106." + _)
    val ZK_QUORUM = ZkHostIP.map(_ + ":2181").mkString(",")
    //    print(ZK_QUORUM)
    val numThreads = 1
    //创建一个streamingContext
//    val spark = SparkSession
//      .builder()
//      .appName("Streaming Frome Kafka")
//      .config("hive.exec.dynamic.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .enableHiveSupport().getOrCreate()
    val sc=new SparkContext(new SparkConf().setAppName("ReceiverFromKafkaWC"))
//    val sc=spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(exectime.toLong))
    val topicMap = topic.split(",").map((_, numThreads)).toMap //每个topic启动的线程
    //通过receiver接收kafka
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    mesR.map((_,1L)).reduceByKey(_+_).print

    ssc.start()
    ssc.awaitTermination()
  }
}
