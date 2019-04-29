package com.huanfion.Spark
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object ReadApacheLog {
  case class ApacheLog(
                      host:String,
                      user:String,
                      password:String,
                      timestamp:String,
                      method:String,
                      endpoint:String,
                      protocol:String,
                      code:Integer,
                      size:Integer
                      )
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ApacheLog").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf)
      .enableHiveSupport().getOrCreate()
    def parse_logline(line:String):Option[ApacheLog]={
      val apache_pattern="""^(\S+) (\S+) (\S+) \[([\w+/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)""".r
      line match {
        case apache_pattern(a,b,c,t,f,g,h,x,y)=>{
          val size=if(y=="-") 0 else y.toInt
          Some(ApacheLog(a,b,c,t,f,g,h,x.toInt,size))
        }
        case _=>None
      }
    }
    import spark.implicits._
    val filename="C:\\Users\\User\\Downloads\\NASA_access_log_Jul95\\access_log_Jul95"
    val rawData=spark.read.text(filename).as[String].cache()
    val apacheLogs=rawData.flatMap(parse_logline)
    import org.apache.spark.sql.functions._
    apacheLogs.filter(col("code")===404).groupBy("endpoint")
      .count.orderBy(desc("count")).limit(10).show()

  }
}
