package com.huanfion.hadoop.mr.secondSort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/*
* 自定义分组类
* */
public class FirstGrouping implements RawComparator<PairWriteable> {
    //这是一种在二进制也就是在字节数组上进行的比较，会更加高效
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1,0,l1-4,b2,0,l2-4);
    }

    @Override
    public int compare(PairWriteable o1, PairWriteable o2) {
        return o1.getName().compareTo(o2.getName());
    }
}
