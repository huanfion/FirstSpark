package com.huanfion.cf

import org.apache.spark.sql.SparkSession
import breeze.numerics.{pow, sqrt}
object ItemBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ItemBase CF")
      .enableHiveSupport().getOrCreate()

    val udata=spark.sql("select user_id,item_id,rating from badou.udata")
    import spark.implicits._

    //计算余弦相似度 cosine=a*b/(|a|*|b|)
    val ItemScoreSum=udata.rdd.map(x=>(x(1).toString,x(2).toString)).groupByKey()
      .mapValues(x=>sqrt(x.toArray.map(rating=>pow(rating.toDouble,2)).sum))
      .toDF("item_id","rating_sqrt_sum")

    val df=udata.selectExpr("user_id ","item_id as item_v","rating as rating_v")
    val df_join=df.join(udata,"user_id").filter("cast(item_v as long) < cast(item_id as long)")

    // 计算两个物品在一个user下的评分的乘积,cosine举例的分子的一部分
    import org.apache.spark.sql.functions._
    val item_udf= udf((x1:Int,x2:Int)=>{
      x1.toDouble * x2.toDouble
    })
    val df_product = df_join.withColumn("rating_product",
      item_udf(col("rating"), col("rating_v")))
      .select("item_id", "item_v", "rating_product")

    val df_group=df_product.groupBy("item_id","item_v")
      .agg("rating_product"->"sum").withColumnRenamed("sum(rating_product)", "rating_dot")
    // |b|
    val ItemScoreSum_b=ItemScoreSum.select("item_v","rating_sqrt_sum_v")

    //计算余弦相似度
    val df_sim=df_group.join(ItemScoreSum,"item_id").join(ItemScoreSum_b,"item_v")
      .selectExpr("item_id","item_v","rating_dot/(rating_sqrt_sum_v*rating_sqrt_sum)  as consine_sim")

    //计算每个物品的前n个相似物品
    val df_nsim=df_sim.rdd.map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey().mapValues{x=>
      x.toArray.sortWith((x,y)=>x._2>y._2).slice(0,10)
    }.flatMapValues(x=>x).toDF("item_id","itemv_sim")
      .selectExpr("item_id","itemv_sim._1 as item_v","itemv_sim._2 as sim")

    //过滤用户购买当前物品时，已经购买的其他物品
    val df_user_item=udata.rdd.map(x=>(x(0).toString,x(1).toString+"_"+x(2).toString))
      .groupByKey().mapValues(x=>x.toArray).toDF("user_id","item_rating_arr")
    val df_user_item_v=df_user_item.selectExpr("user_v","item_rating_arr_v")
    val simRatingUDF = udf((items: Seq[String], items_v: Seq[String]) => {
      val fMap = items.map { x =>
        val s = x.split("_")
        (s(0), s(1))
      }.toMap
      items_v.filter { x =>
        val s = x.split("_")
        fMap.getOrElse(s(0), -1) == -1
      }
    })
//    val df_filter_item=df_nsim.join(df_user_item,"item_id").withColumn("item_pro",simRatingUDF
  }
}
