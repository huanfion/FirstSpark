package com.huanfion.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("User Base CF")
      .enableHiveSupport().getOrCreate()

    val udata = spark.sql("select * from badou.udata");
    //    1.计算余弦相似度 cosine=a*b/(|a|*|b|)
    import spark.implicits._
    val userScoreSum = udata.rdd
      .map(x => (x(0).toString(), x(2).toString()))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum))
      .toDF("user_id", "rating_sqrt_sum")
    /*
    * +-------+------------------+
|user_id|   rating_sqrt_sum|
+-------+------------------+
|    273|17.378147196982766|
|    528|28.160255680657446|
|    584|17.916472867168917|
|    736|16.186414056238647|
|    456| 52.40229002629561|
|    312| 66.83561924602779|
|     62|  53.0659966456864|
|    540|29.866369046136157|
|    627| 46.62617290749907|
|    317| 17.08800749063506|
|    734|  29.9833287011299|
|    795|41.036569057366385|
|    436| 46.45427859734774|
|     41|27.784887978899608|
|    717|37.376463182061514|
|    672|18.439088914585774|
|    279| 72.49827584156743|
|    191| 19.28730152198591|
|    762|15.874507866387544|
|    193| 36.89173349139343|
+-------+------------------+
*/

    //    1.1 item->user倒排表
    var df = udata.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
    val df_join = df.join(udata, "item_id").filter("cast(user_id as long) <> cast(user_v as long)")
    //    1.2计算两个用户在一个item下的评分的乘积,cosine举例的分子的一部分
    import org.apache.spark.sql.functions._
    val product_udf = udf((x1: Int, x2: Int) => {
      x1.toDouble * x2.toDouble
    })
    val df_product = df_join.withColumn("rating_product",
      product_udf(col("rating"), col("rating_v")))
      .select("user_id", "user_v", "rating_product")
    //    计算完整的分子部分
    val df_sim_group = df_product.groupBy("user_id", "user_v")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rating_dot")
    val userScoreSum_v = userScoreSum.selectExpr("user_id as user_v", "rating_sqrt_sum as rating_sqrt_sum_v")
    //    1.3计算余弦相似度
    val df_sim = df_sim_group.join(userScoreSum, "user_id").join(userScoreSum_v, "user_v")
      .selectExpr("user_id", "user_v", "rating_dot/(rating_sqrt_sum*rating_sqrt_sum_v) as consine_sim")

    // 2.获取相似用户的物品集合
    //2.1取得前n个相似用户
    val df_nsim = df_sim.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey().mapValues { x =>
      x.toArray.sortWith((x, y) => x._2 > y._2).slice(0, 10)
    }.flatMapValues(x => x).toDF("user_id", "user_v_sim")
      .selectExpr("user_id", "user_v_sim._1 as user_v", "user_v_sim._2 as sim")
    //2.2获取用户的物品集合进行过滤
    val df_user_item = udata.rdd.map(x => (x(0).toString, x(1).toString + "_" + x(2).toString))
      .groupByKey().mapValues(x => x.toArray)
      .toDF("user_id", "item_rating_arr")
    //过滤
    val df_user_item_v = df_user_item.selectExpr("user_id as user_v", "item_rating_arr as item_rating_arr_v")
    //2.3用一个udf过滤相似用户user_id1中包含user_id已经打过分的商品
    val filter_udf = udf((items: Seq[String], items_v: Seq[String]) => {
      val fMap = items.map { x =>
        val s = x.split("_")
        (s(0), s(1))
      }.toMap
      items_v.filter { x =>
        val s = x.split("_")
        fMap.getOrElse(s(0), -1) == -1
      }
    })
    val df_gen_item = df_nsim.join(df_user_item,"user_id").join(df_user_item_v,"user_v")
    val df_filter_item = df_gen_item
      .withColumn("filtered_item",
        filter_udf(col("item_rating_arr"), col("item_rating_arr_v")))
      .select("user_id", "sim", "filtered_item")
    //2.4相似度*rating
    val simRatingUDF = udf { (sim: Double, items: Seq[String]) =>
      items.map { x =>
        val s = x.split("_")
        s(0) + "_" + s(1).toDouble * sim
      }
    }
    val ItemSimRating = df_filter_item.withColumn("item_pro", simRatingUDF(col("sim"), col("filtered_item"))).select("user_id", "item_pro")

    val userItemScore = ItemSimRating.select(ItemSimRating("user_id")
      , explode(ItemSimRating("item_pro"))).toDF("user_id", "item_pro")
      .selectExpr("user_id", "split(item_pro,'_')[0] as item_id",
        "cast (split(item_pro,'_')[1] as double) as score")

    userItemScore.filter("user_id=71")
  }
}
