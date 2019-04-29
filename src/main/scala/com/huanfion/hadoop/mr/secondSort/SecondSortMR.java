package com.huanfion.hadoop.mr.secondSort;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * 二次排序
 * 二次排序的步骤
 * 1.自定义key，把key和value联合起来作为一个新的key，记作newkey。这时newkey含有两个字段，为k,v。value还是原来的value
 * 2.自定义分区类,继承Partitionner
 * 3.自定义分组函数类，需要继承WritableComparator
 */
public class SecondSortMR extends Configured implements Tool {


    public static class WordCountMapper extends Mapper<LongWritable, Text, PairWriteable, FloatWritable> {
        private PairWriteable outputKey=new PairWriteable();
        private FloatWritable outputValue=new FloatWritable();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values=value.toString().split(" ");
            if(2!=values.length){
                return;
            }
            outputKey.set(values[0],Float.valueOf(values[1]));
            outputValue.set(Float.valueOf(values[1]));
            context.write(outputKey,outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * 这里是MR程序 reducer阶段处理的类
     * KEYIN：就是reducer阶段输入的数据key类型，对应mapper的输出key类型  在本案例中  就是单词  Text
     * VALUEIN就是reducer阶段输入的数据value类型，对应mapper的输出value类型  在本案例中  就是单词次数  IntWritable
     * KEYOUT就是reducer阶段输出的数据key类型 在本案例中  就是单词  Text
     * VALUEOUTreducer阶段输出的数据value类型 在本案例中  就是单词的总次数  IntWritable
     */
    public static class WordCountReducer extends Reducer<PairWriteable, FloatWritable, Text, FloatWritable> {
        private Text outputKey=new Text();
        @Override
        protected void reduce(PairWriteable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            for (FloatWritable value : values) {
                outputKey.set(key.getName());
                context.write(outputKey, value);
            }

        }
    }

    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputvalue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义一个计数器
            int count = 0;
            List<IntWritable> list = Lists.newArrayList(values);
            System.out.println("combiner==keyIn" + key + "   valueIn:" + list);
            for (IntWritable value : list) {
                count += value.get();
            }
            outputvalue.set(count);
            System.out.println("combiner==keyout" + key + "   valueout:" + outputvalue);
            context.write(key, outputvalue);
        }
    }

    public int run(String[] args) throws Exception {
        //1. get conf
        Configuration configuration = this.getConf();
        //2. get job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        //指定本次mr job jar包运行主类
        job.setJarByClass(this.getClass());
        //3.1 input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //3.2 mapper 指定本次mr 所用的mapper 类是什么以及map阶段的输出  k  v类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(PairWriteable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        //1.设置partition,分区
         job.setPartitionerClass(FirstPartitioner.class);
        //2.排序
        //job.setSortComparatorClass();
        //3.如果业务有需求，就可以设置combiner组件
        //job.setCombinerClass(WordCountCombiner.class);
        //4.compress 压缩
        //一般企业中用的比较多的是SnappyCodec和Lz4Codec
//        configuration.set("mapreduce.map.output.compress", "true");
//        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        //org.apache.hadoop.io.compress.Lz4Codec
        //5.group
        job.setGroupingComparatorClass(FirstGrouping.class);

        //设置reduce数量，需要合理的设置
        // job.setNumReduceTasks(3); //ReduceTask个数
        //3.3 reducer 指定本次mr 所用的mreducer类是什么以及最终输出的k v类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);


        //3.4 output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //3.5提交
        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }

    public static void main(String[] args) {
        //本地跑加上这行代码
        System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.6.0-bin-master");
        SecondSortMR mr = new SecondSortMR();
        Configuration configuration = new Configuration();
        try {
            args = new String[]{"hdfs://master:9000/data/secondSort.txt", "hdfs://master:9000/mr/java/secondSortResult"};
            //判断输出地址是否已经存在，存在则删除
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
            int status = ToolRunner.run(configuration, mr, args);
            System.exit(status);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
