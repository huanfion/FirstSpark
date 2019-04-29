package com.huanfion.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
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
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

public class WordCountMR {

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
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到传入的一行数据，转化为String
            String line = value.toString();
            //将这一行内容按照分隔符切割成字符串数组。
            String[] Words = line.split(" ");
            System.out.println("key:"+key+"=== value:"+value);
            for (String word : Words) {
                //使用mr程序的上下文context 把mapper阶段处理的数据发送出去
                //作为reduce节点的输入数据
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /**
     * 这里是MR程序 reducer阶段处理的类
     * KEYIN：就是reducer阶段输入的数据key类型，对应mapper的输出key类型  在本案例中  就是单词  Text
     * VALUEIN就是reducer阶段输入的数据value类型，对应mapper的输出value类型  在本案例中  就是单词次数  IntWritable
     * KEYOUT就是reducer阶段输出的数据key类型 在本案例中  就是单词  Text
     * VALUEOUTreducer阶段输出的数据value类型 在本案例中  就是单词的总次数  IntWritable
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义一个计数器
            int count = 0;
            List<IntWritable> list=Lists.newArrayList(values);
            System.out.println("ReducekeyIN:"+key+"=== value:"+list);
            for (IntWritable value : list) {
                count += value.get();
            }
            outputValue.set(count);
            context.write(key, outputValue);
        }
    }

    public void run(String[] args) throws Exception {
        //1.get conf
        Configuration configuration = new Configuration();
        //2.get job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        //指定本次mr job jar包运行主类
        job.setJarByClass(this.getClass());
        //3.1 input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //3.2 mapper 指定本次mr 所用的mapper 类是什么以及map阶段的输出  k  v类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.3 reducer 指定本次mr 所用的mreducer类是什么以及最终输出的k v类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // job.setNumReduceTasks(3); //ReduceTask个数
        //如果业务有需求，就可以设置combiner组件
        //job.setCombinerClass(WordCountReducer.class);
        //3.4 output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //3.5提交
        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

    public static void main(String[] args) {
        //本地跑加上这行代码
        System.setProperty("hadoop.home.dir", "D:\\Soft\\hadoop-common-2.6.0-bin-master");
        WordCountMR mr = new WordCountMR();
        try {
            args=new String[]{"hdfs://master:9000/mrtest.txt", "hdfs://master:9000/mr/java/wordcount"};
            FileSystem fileSystem=FileSystem.get(new Configuration());
            Path fileoutpath=new Path(args[1]);
            if(fileSystem.exists(fileoutpath)){
                fileSystem.delete(fileoutpath,true);
            }
            mr.run(args);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
