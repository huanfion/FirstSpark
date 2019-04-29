package com.huanfion.Streaming

import org.apache.spark.{SparkConf, SparkContext}
/*https://blog.csdn.net/wtzhm/article/details/80864018*/
object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("broadcastdemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /*
    1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    * */
    val rdd = sc.textFile("file:\\D:\\Work\\BigData\\FirstSpark\\src\\main\\scala\\com\\huanfion\\Streaming\\ip.txt")
    val rdd1 = rdd
      .map(line => {
        val fields = line.split("\\|")
        val start_num = fields(2)
        val end_num = fields(3)
        val province = fields(6)
        (start_num, end_num, province)
      })
    val rpRulesBroadcast = rdd.collect()
    val ipRulesBroadcast = sc.broadcast(rpRulesBroadcast)

    val rdd3=rdd.map(line=>{
      val fileds=line.split("\\|")
      fileds(1)
    })
    //Ip地址转换成十进制数字
    def ip2Long(ip: String): Long = {
      val fragments = ip.split("[.]")
      var ipNum = 0L
      for (i <- 0 until fragments.length) {
        ipNum = fragments(i).toLong | ipNum << 8L
      }
      ipNum
    }
    //在每个task中获取广播值，进行查询
    val result=rdd3.map(ip=>{
      val ipNum = ip2Long(ip)
    })
  }
}
