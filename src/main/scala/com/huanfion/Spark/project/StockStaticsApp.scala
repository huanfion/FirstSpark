package com.huanfion.Spark.project

import org.apache.spark.sql.SparkSession

/*
1.计算所有订单中每年的销售单数、销售总额
2.计算所有订单每年最大金额订单的销售额
3.计算所有订单中每年最畅销货品
 */
object StockStaticsApp {

  //订单类
  case class tbStock(ordernumber: String, locationid: String, dateid: String)

  //订单明细类
  case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double)

  //日期类
  case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("StockStaticsApp").getOrCreate()
    import spark.implicits._
    val StockRDD = spark.sparkContext.textFile("hdfs://master:9000/home/huanfion/data/Stock/tbStock.txt")
    val StockDetailRDD = spark.sparkContext.textFile("hdfs://master:9000/home/huanfion/data/Stock/tbStockDetail.txt")
    val DateRDD = spark.sparkContext.textFile("hdfs://master:9000/home/huanfion/data/Stock/tbDate.txt")
    val StockDS=StockRDD.map(_.split(",")).map(x=>tbStock(x(0),x(1),x(2))).toDS
    val StockDetailDS=StockDetailRDD.map(_.split(","))
      .map(x=>tbStockDetail(x(0),x(1).trim.toInt,x(2),x(3).trim.toInt,x(4).trim.toDouble,x(5).trim.toDouble)).toDS()
    val DateDS=DateRDD.map(_.split(","))
      .map(x=>tbDate(x(0),x(1).trim.toInt,x(2).trim.toInt,x(3).trim.toInt,x(4).trim.toInt,x(5).trim.toInt,x(6).trim.toInt,x(7).trim.toInt,x(8).trim.toInt,x(9).trim.toInt))
        .toDS()
//    StockDS.show()
//    StockDetailDS.show()
//    DateDS.show()
    StockDS.createOrReplaceTempView("tbStock")
    StockDetailDS.createOrReplaceTempView("tbStockDetail")
    DateDS.createOrReplaceTempView("tbDate")

    //1.计算所有订单中每年的销售单数、销售总额
//    spark.sql("select  c.theyear,count(distinct a.ordernumber) as OrderCount,sum(b.amount) as OrderAmount " +
//      "from tbStock a j" +
//      "oin tbStockDetail b on a.ordernumber=b.ordernumber " +
//      "join tbDate c on a.dateid=c.dateid " +
//      "group by c.theyear order by c.theyear").show()
    //2.计算所有订单每年最大金额订单的销售额
    //2.1统计每年，每个订单的销售额
    //select a.dateid,a.ordernumber,sum(b.amount) as SumOfAmount from tbStock a join tbStockDetail b on a.ordernumber=b.ordernumber group by a.dateid,a.ordernumber
    //2.2 以2.1计算出来的结果为基础，和表tbDate join，求出每年最大金额订单的销售额
   //spark.sql("select theyear,max(c.SumOfAmount) as MaxAmount from (select a.dateid,a.ordernumber,sum(b.amount) as SumOfAmount from tbStock a join tbStockDetail b on a.ordernumber=b.ordernumber group by a.dateid,a.ordernumber) c join tbDate d on c.dateid=d.dateid group by theyear order by theyear desc").show()
    //3.计算所有订单中每年最畅销货品
//    第一步：
//    select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid;
//    第二步:
//      select d.theyear,max(d.sumofamount) as maxofamount from (select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid) d group by d.theyear ;
//    第三步：
//    select distinct  e.theyear,e.itemid,f.maxofamount from (select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid) e , (select d.theyear,max(d.sumofamount) as maxofamount from (select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid) d group by d.theyear) f where e.theyear=f.theyear and e.sumofamount=f.maxofamount order by e.theyear;
    spark.sql("select distinct  e.theyear,e.itemid,f.maxofamount from (select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid) e , (select d.theyear,max(d.sumofamount) as maxofamount from (select c.theyear,b.itemid,sum(b.amount) as sumofamount from tbStock a,tbStockDetail b,tbDate c where a.ordernumber=b.ordernumber and a.dateid=c.dateid group by c.theyear,b.itemid) d group by d.theyear) f where e.theyear=f.theyear and e.sumofamount=f.maxofamount order by e.theyear;").show()
 }
}
