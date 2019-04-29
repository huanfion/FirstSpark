package com.huanfion.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
 *    mapreduce只接受writeable类型
 *    用户自定义Writeable类型
 * */
public class UserWriteable implements Writable {
    private int id;
    private String name;

    public UserWriteable(int id, String name) {
        set(id, name);
    }

    public void set(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        /*
         * 注意这里的顺序需要和声明的变量顺序一致
         * */
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /*
     * 重写equals hashCode toString 方法
     * */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserWriteable)) return false;
        UserWriteable that = (UserWriteable) o;
        return id == that.id &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "UserWriteable{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
