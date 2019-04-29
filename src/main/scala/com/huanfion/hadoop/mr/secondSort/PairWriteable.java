package com.huanfion.hadoop.mr.secondSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWriteable implements WritableComparable<PairWriteable> {
    private String name;//要比较的第一个字段，姓名
    private float money;//要比较的第二个字段，消费金额

    public  PairWriteable(){

    }
    public PairWriteable(String name, float money) {
        this.name = name;
        this.money = money;
    }

    public void set(String name, float money) {
        this.name = name;
        this.money = money;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getMoney() {
        return money;
    }

    public void setMoney(float money) {
        this.money = money;
    }

    @Override
    public int compareTo(PairWriteable o) {
        //先比较name，在比较money
        int comp = this.name.compareTo(o.getName());
        if (0 != comp) {
            return comp;
        }
        return Float.valueOf(this.getMoney()).compareTo(Float.valueOf(o.getMoney()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeFloat(money);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.money = in.readFloat();
    }
}