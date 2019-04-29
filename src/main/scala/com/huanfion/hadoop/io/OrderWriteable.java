package com.huanfion.hadoop.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * 自定义类型实现WritableComparable
 * */
public class OrderWriteable implements WritableComparable<OrderWriteable> {
    private String orderId;
    private float price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public OrderWriteable(String orderId, float price) {
        set(orderId, price);
    }

    public void set(String orderId, float price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderWriteable o) {
        int compare = this.getOrderId().compareTo(o.getOrderId());
        if (compare == 0) {
            compare = Float.valueOf(price).compareTo(Float.valueOf(o.getPrice()));
        }
        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeFloat(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.price=dataInput.readFloat();
    }
}
