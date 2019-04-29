package com.huanfion.Spark;

import com.sun.tools.internal.ws.processor.model.java.JavaParameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationJava {
    public static void main(String[] args) {
        //map();
//        filter();
        //flatMap();
//        groupByKey();
//        reduceByKey();
//        sortByKey();
//        join();
        cogroup();
    }
    public static void cogroup()
    {
        List stulist=Arrays.asList(
                new Tuple2<>(1,"liuda"),
                new Tuple2<>(2,"lier"),
                new Tuple2<>(3,"zhangsan"),
                new Tuple2<>(4,"gousi"),
                new Tuple2<>(5,"lily"),
                new Tuple2<>(6,"lucy"));
        List scorelist=Arrays.asList(
                new Tuple2<>(1,88),
                new Tuple2<>(2,87),
                new Tuple2<>(2,88),
                new Tuple2<>(2,97),
                new Tuple2<>(3,90),
                new Tuple2<>(3,50),
                new Tuple2<>(4,100),
                new Tuple2<>(5,58),
                new Tuple2<>(6,65));
        JavaSparkContext sc=getsc();
        JavaPairRDD sturdd=sc.parallelizePairs(stulist);
        JavaPairRDD scorerdd=sc.parallelizePairs(scorelist);

        JavaPairRDD<Integer,Tuple2<Iterable,Iterable>> cogroupvalue=sturdd.cogroup(scorerdd);

        cogroupvalue.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable, Iterable>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable, Iterable>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1+":"+integerTuple2Tuple2._2._2);
            }
        });
    }
    public static void join()
    {
        List stulist=Arrays.asList(
                new Tuple2<>(1,"liuda"),
                new Tuple2<>(2,"lier"),
                new Tuple2<>(3,"zhangsan"),
                new Tuple2<>(4,"gousi"),
                new Tuple2<>(5,"lily"),
                new Tuple2<>(6,"lucy"));
        List scorelist=Arrays.asList(
                new Tuple2<>(1,88),
                new Tuple2<>(2,87),
                new Tuple2<>(3,90),
                new Tuple2<>(4,100),
                new Tuple2<>(5,58),
                new Tuple2<>(6,65));
        JavaSparkContext sc=getsc();
        JavaPairRDD sturdd=sc.parallelizePairs(stulist);
        JavaPairRDD scorerdd=sc.parallelizePairs(scorelist);

        JavaPairRDD<Integer,Tuple2<String,Integer>> joinvalue=sturdd.join(scorerdd);

        joinvalue.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1+":"+integerTuple2Tuple2._2._2);
            }
        });
    }
    public static void sortByKey(){
        List list=Arrays.asList(
                new Tuple2<>(91,"liuda"),
                new Tuple2<>(78,"lier"),
                new Tuple2<>(99,"zhangsan"),
                new Tuple2<>(76,"gousi"),
                new Tuple2<>(90,"lily"),
                new Tuple2<>(89,"lucy"));
        JavaPairRDD rdd=getsc().parallelizePairs(list);
        JavaPairRDD<Integer,String> sortvalue=rdd.sortByKey(false);
        sortvalue.foreach(x->System.out.println(x._1+"--"+x._2));
    }
    public static void reduceByKey(){
        List list=Arrays.asList(
                new Tuple2<>("class_1",91),
                new Tuple2<>("class_2",78),
                new Tuple2<>("class_1",99),
                new Tuple2<>("class_2",76),
                new Tuple2<>("class_2",90),
                new Tuple2<>("class_1",86));
        JavaPairRDD rdd=getsc().parallelizePairs(list);
        JavaPairRDD<String,Iterable> reducevalues=rdd.reduceByKey(new Function2<Integer, Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        reducevalues.foreach(x->System.out.println(x._1+"--"+x._2));
    }
    public static void groupByKey(){
        List list=Arrays.asList(
                new Tuple2<>("class_1",90),
                new Tuple2<>("class_2",78),
                new Tuple2<>("class_1",99),
                new Tuple2<>("class_2",76),
                new Tuple2<>("class_2",90),
                new Tuple2<>("class_1",86));
        JavaPairRDD rdd=getsc().parallelizePairs(list);

        JavaPairRDD<String,Iterable> groupvalue=rdd.groupByKey();
        groupvalue.foreach(x->System.out.println(x._1+"--"+x._2));
    }
    public static void flatMap(){
        List list=Arrays.asList("Hadoop Hive","Hadoop Hbase");
        JavaRDD rdd=getsc().parallelize(list);
        JavaRDD flatMapValue=rdd.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator call(String value) throws Exception {
                return Arrays.asList(value.split(" ")).iterator();
            }
        });
        flatMapValue.foreach(x->System.out.println(x));
    }
    public static void map(){
        JavaSparkContext sc=getsc();

        List list= Arrays.asList(1,2,3,4);

        JavaRDD rdd=sc.parallelize(list);

        JavaRDD count=rdd.map(new Function<Integer,Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value * 10;
            }
        });
        count.foreach(x->System.out.println(x));
    }

    public static void filter(){
        JavaSparkContext sc=getsc();

        List list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD rdd=sc.parallelize(list);

        JavaRDD filterValue=rdd.filter(x->(Integer) x%2==0);

        filterValue.foreach(x->System.out.println(x));
    }

    public static JavaSparkContext getsc()
    {
        SparkConf sparkconf=new SparkConf().setAppName("Transformation").setMaster("local");

        JavaSparkContext sc=new JavaSparkContext(sparkconf);

        return sc;
    }
}
