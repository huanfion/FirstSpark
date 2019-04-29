package com.huanfion.Spark.project;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class LogApp {
    public static JavaSparkContext GetSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("LogApp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static void main(String[] args) {
        JavaSparkContext sc = GetSparkContext();
        JavaRDD<String> rdd = sc.textFile("hdfs://master:9000/data/access.log");

        //rdd 映射到key value对
        JavaPairRDD<String, LogInfo> logPairRDD = rdd.mapToPair(new PairFunction<String, String, LogInfo>() {
            @Override
            public Tuple2<String, LogInfo> call(String s) throws Exception {
                long timestamp = Long.valueOf(s.split("\t")[0]);
                String deviceID = s.split("\t")[1];
                long uptraffic = Long.valueOf(s.split("\t")[2]);
                long downtraffic = Long.valueOf(s.split("\t")[3]);
                LogInfo logInfo = new LogInfo(timestamp, uptraffic, downtraffic);
                return new Tuple2(deviceID, logInfo);
            }
        });

        JavaPairRDD<String, LogInfo> aggrRDD = aggreByDeviceID(logPairRDD);
        //JavaPairRDD<LogSort,LogInfo>-》JavaPairRDD<LogSort，String>，需要根据logInfo排序
        JavaPairRDD<LogSort, String> sortPairRDD = aggrRDD.mapToPair(new PairFunction<Tuple2<String, LogInfo>, LogSort, String>() {
            @Override
            public Tuple2<LogSort, String> call(Tuple2<String, LogInfo> stringLogInfoTuple2) throws Exception {
                Long timeStamp = stringLogInfoTuple2._2.getTimestamp();
                String deviceID = stringLogInfoTuple2._1;
                Long upTraffic = stringLogInfoTuple2._2.getUpTraffic();
                Long downTraffic = stringLogInfoTuple2._2.getDownTraffic();
                LogSort logSort = new LogSort(timeStamp, upTraffic, downTraffic);
                return new Tuple2(logSort, deviceID);

            }
        });
        JavaPairRDD<LogSort, String> sortRDD = sortPairRDD.sortByKey(false);
        List<Tuple2<LogSort, String>> list = sortRDD.take(10);
        for (Tuple2<LogSort, String> logSortStringTuple2 : list) {
            System.out.println(logSortStringTuple2._2 + ":" +
                    logSortStringTuple2._1.getUpTraffic() + ":" +
                    logSortStringTuple2._1.getDownTraffic());
        }
    }

    /**
     * 根据deviceID 聚合
     *
     * @param mapPairRDD
     * @return
     */
    private static JavaPairRDD<String, LogInfo> aggreByDeviceID(JavaPairRDD<String, LogInfo> mapPairRDD) {
        return mapPairRDD.reduceByKey(new Function2<LogInfo, LogInfo, LogInfo>() {
            @Override
            public LogInfo call(LogInfo v1, LogInfo v2) throws Exception {
                //取最小的时间戳
                long timestamp = (v1.getTimestamp() < v2.getTimestamp()) ? v1.getTimestamp() : v2.getTimestamp();
                long upTraffic = v1.getUpTraffic() + v2.getUpTraffic();
                long downTraffic = v1.getDownTraffic() + v2.getDownTraffic();
                LogInfo logInfo = new LogInfo();
                logInfo.setTimestamp(timestamp);
                logInfo.setUpTraffic(upTraffic);
                logInfo.setDownTraffic(downTraffic);
                return logInfo;
            }
        });
    }
}
