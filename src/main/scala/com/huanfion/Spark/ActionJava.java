package com.huanfion.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionJava {
    public static JavaSparkContext getsc() {
        SparkConf conf = new SparkConf().setAppName("action").setMaster("local");
        return new JavaSparkContext(conf);
    }

    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();
        countByKey();
    }

    public static void reduce() {
        List list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = getsc().parallelize(list);
        int reducevalue = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reducevalue);
    }

    public static void collect() {
        List list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = getsc().parallelize(list);
        System.out.println(rdd.collect());
    }

    public static void save() {
        List list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = getsc().parallelize(list);
        rdd.saveAsTextFile("hdfs://");//此处的hdfs目录路径必须存在
    }

    public static void count() {
        List list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = getsc().parallelize(list);
        System.out.println(rdd.count());
    }

    public static void take() {
        List list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = getsc().parallelize(list);
        List<Integer> takevalue = rdd.take(2);
        System.out.println(takevalue);
    }

    public static void countByKey() {
        List list = Arrays.asList(new Tuple2<>("class_1", 91),
                new Tuple2<>("class_2", 78),
                new Tuple2<>("class_1", 99),
                new Tuple2<>("class_2", 76),
                new Tuple2<>("class_2", 90));
        JavaPairRDD<String, Integer> rdd = getsc().parallelizePairs(list);
        Map<String, Long> values = rdd.countByKey();
        values.forEach((x, y) -> System.out.println(x + ":" + y));
    }
}
