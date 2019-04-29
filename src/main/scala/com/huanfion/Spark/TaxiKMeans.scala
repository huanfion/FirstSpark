package com.huanfion.Spark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types._

/*
通过分析出租车数据，使用KMeans对经纬度进行聚类，然后按照（类型，时间）进行分类，再统计每个类别每个时段的次数
 */
object TaxiKMeans {
  def main(args: Array[String]): Unit = {
    if(args.length<2)
      {
        System.err.println("Useage :TaxiKMeans less Arguments")
        System.exit(1)
      }
    val spark=SparkSession.builder().appName("Taxi").master("local[*]").getOrCreate()
    //val spark=SparkSession.builder().appName("Taxi").master("spark://master:7077").getOrCreate()
    //创建schema
    val schema=StructType(Array(
      StructField("id",IntegerType,true),
      StructField("lng",DoubleType,true),
      StructField("lat",DoubleType,true),
      StructField("time",StringType,true)
    ))
    val path="file:///home/badou/Documents/data/Spark/taxi.csv"
    val data=spark.read.schema(schema).csv(path)
    //将lng lat转换成训练使用的Vector类型
    val assembler=new VectorAssembler()
    val lng_lat=Array("lng","lat")
    assembler.setInputCols(lng_lat).setOutputCol("feature").transform(data)
    //按照8:2比例随机分割数据用于训练和测试
    val splitRait=Array(0.8,0.2)
    val Array(train,test)=data.randomSplit(splitRait)
    //建立Kmeans,设置类别数为10
    val kMeans=new KMeans().setK(args(0).toInt).setFeaturesCol("feature").setPredictionCol("prediction")
    //训练模型
    val model=kMeans.fit(train)
    //测试数据
    val testResult=model.transform(test)

    //导入隐式转换，不然time会出现错误 not a member of StringContext
    import spark.implicits._
    val time_prediction = testResult.select($"time".substr( 0, 2).alias("hour"), $"prediction")
    //"hdfs://master:9000/output/taxiresult"
    time_prediction.write.mode(SaveMode.Overwrite).save(args(1))
    import org.apache.spark.sql.functions._
    time_prediction.groupBy("hour","prediction").agg(("prediction","count")).orderBy(desc("count"))
      .filter(r=>r.getAs(0)==15).take(10)
    val df=spark.sql("select * from userdb.orders")
    val orderNumberSort=df.select("user_id","order_number","order_hour_of_day").rdd.map( x=>(x(0).toString,(x(1).toString,x(2).toString))).groupByKey()
    df.select("user_id","order_number","order_hour_of_day").groupBy("user_id")

  }
}
