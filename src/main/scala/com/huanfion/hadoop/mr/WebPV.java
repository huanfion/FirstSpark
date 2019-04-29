package com.huanfion.hadoop.mr;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * wordcount mr  使用toolruner优化  ,新增配置项
 */
public class WebPV extends Configured implements Tool {

    /**
     * 这里就是mapreduce程序  mapper阶段业务逻辑实现的类
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     * KEYIN：表示mapper数据输入的时候key的数据类型，在默认的读取数据组件下，叫InputFormat,它的行为是一行一行的读取待处理的数据
     * 读取一行，返回一行给我们的mr程序，这种情况下  keyin就表示每一行的起始偏移量  因此数据类型是Long
     * VALUEIN:表述mapper数据输入的时候value的数据类型，在默认的读取数据组件下 valuein就表示读取的这一行内容  因此数据类型是String
     * KEYOUT 表示mapper数据输出的时候key的数据类型  在本案例当中 输出的key是单词  因此数据类型是 String
     * VALUEOUT表示mapper数据输出的时候value的数据类型  在本案例当中 输出的key是单词的次数  因此数据类型是 Integer
     * 这里所说的数据类型String Long都是jdk自带的类型   在序列化的时候  效率低下 因此hadoop自己封装一套数据类型
     * long---->LongWritable
     * String-->Text
     * Integer--->Intwritable
     * null-->NullWritable
     */
    public static class WebPVMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            //values长度不够24，自然取不到provinceId
            if (values.length < 24) {
                //统计输入数据长度小于24，导致数据无效的数量
                context.getCounter("WEBPV_COUNTERS","LENGTH_LESS24_COUNTER").increment(1);
                return;
            }
            String provinceIdvalue = values[23];
            String url = values[1];
            //如果url为空，后面取得值自然无意义
            if (StringUtils.isBlank(url)) {
                context.getCounter("WEBPV_COUNTERS","URL_ISBLACK_COUNTER").increment(1);
                return;
            }
            if (StringUtils.isBlank(provinceIdvalue)) {
                context.getCounter("WEBPV_COUNTERS","PROVINCEID_ISBLACK_COUNTER").increment(1);
                return;
            }
            int provinceId = 0;
            try {
                provinceId = Integer.valueOf(provinceIdvalue);

            } catch (Exception ex) {
                context.getCounter("WEBPV_COUNTERS","PROVINCEID_VALIDATE_COUNTER").increment(1);
                return;
            }
            context.write(new IntWritable(provinceId), new IntWritable(1));
        }
    }

    /**
     * 这里是MR程序 reducer阶段处理的类
     * KEYIN：就是reducer阶段输入的数据key类型，对应mapper的输出key类型  在本案例中  就是单词  Text
     * VALUEIN就是reducer阶段输入的数据value类型，对应mapper的输出value类型  在本案例中  就是单词次数  IntWritable
     * KEYOUT就是reducer阶段输出的数据key类型 在本案例中  就是单词  Text
     * VALUEOUTreducer阶段输出的数据value类型 在本案例中  就是单词的总次数  IntWritable
     */
    public static class WebPVReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable outputvalue=new IntWritable();
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum+=value.get();
            }
            outputvalue.set(sum);
            context.write(key,outputvalue);
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
        job.setMapperClass(WebPVMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //1.设置partition,分区
        // job.setPartitionerClass();
        //2.排序
        //job.setSortComparatorClass();
        //3.如果业务有需求，就可以设置combiner组件
        job.setCombinerClass(WebPVReducer.class);
        //4.compress 压缩
        //一般企业中用的比较多的是SnappyCodec和Lz4Codec
//        configuration.set("mapreduce.map.output.compress", "true");
//        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        //org.apache.hadoop.io.compress.Lz4Codec
        //5.group
        //job.setGroupingComparatorClass();

        //设置reduce数量，需要合理的设置
        // job.setNumReduceTasks(3); //ReduceTask个数
        //3.3 reducer 指定本次mr 所用的mreducer类是什么以及最终输出的k v类型
        job.setReducerClass(WebPVReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


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
        WebPV mr = new WebPV();
        Configuration configuration = new Configuration();
        try {
            args = new String[]{"hdfs://master:9000/data/2015082818", "hdfs://master:9000/mr/java/webpv"};
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
