package com.huanfion.Streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object DirectKafkaWC {
  def main(args: Array[String]): Unit = {
      if(args.length<3){
        System.err.println(" Need 3 arguments")
        System.exit(1)
      }
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokerlist,topic,checkpointpath)=args

    val sparkConf=new SparkConf().setAppName("DirectKafkaWC")
    val ssc=new StreamingContext(sparkConf,Seconds(10))
    val topicSet=topic.split(",").toSet
    val kafkaParam=Map[String,String](
      "bootstrap.servers"->brokerlist,
      "group.id"->"group_test",
      "auto.offset.reset"->"smallest"
    )
    // 直连方式拉取数据，这种方式不会修改数据的偏移量，需要手动的更新
    val lines=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParam,topicSet).map(_._2)

    val ds=lines.flatMap(_.split(" ")).map((_,1))

    //全局统计，需要设置checkpoint"hdfs://master:9000/streaming/checkpoint"
    ssc.checkpoint(checkpointpath)
    val ds2=ds.updateStateByKey[Int]((x:Seq[Int],y:Option[Int])=>{
      Some(x.sum+y.getOrElse(0))
    })
    ds2.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
