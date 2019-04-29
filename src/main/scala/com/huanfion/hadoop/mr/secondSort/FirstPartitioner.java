package com.huanfion.hadoop.mr.secondSort;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*自定义分区类
* */
public class FirstPartitioner extends Partitioner<PairWriteable, FloatWritable> {
    @Override
    public int getPartition(PairWriteable pairWriteable, FloatWritable floatWritable, int numPartitions) {
        return (pairWriteable.hashCode())&2147483647 %numPartitions;
    }
}
