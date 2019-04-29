package com.huanfion.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class MyUDF extends UDF {
    ///First, you need to create a new class that extends UDF, with one or more methods named evaluate.
    public Text evaluate(Text s) {
        if (s == null) {
            return null;
        }
        return new Text(s.toString().toUpperCase());
    }
}