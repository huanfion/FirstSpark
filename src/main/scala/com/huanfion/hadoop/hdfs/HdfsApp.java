package com.huanfion.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;

public class HdfsApp {
    public static void main(String[] args) {
        String filePath = "hdfs://master:9000/The_Man_of_Property.txt";
        //读取hdfs文件
        HdfsApp hdfsApp = new HdfsApp();
        /*hdfsApp.readHDFSFile(filePath);*/
       //写入hdfs文件
        hdfsApp.writeHDFS("D:\\Work\\BigData\\FirstSpark\\src\\main\\resources\\hdfs-site.xml","hdfs://master:9000/hdfs-site.txt");
    }

    //获取fileSystem
    private FileSystem getFileSystem() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    public void readHDFSFile(String filePath) {
        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = new Path(filePath);
            fsDataInputStream = this.getFileSystem().open(path);
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }

    public void writeHDFS(String localpath, String hdfspath) {
        FSDataOutputStream fsDataOutputStream = null;
        FileInputStream fileInputStream = null;//这里读取的是本地文件，自然不需要FSDataInputStream，只需要本地文件系统的FileInputStream
        try {
            Path path = new Path(hdfspath);
            fsDataOutputStream = this.getFileSystem().create(path);
            fileInputStream = new FileInputStream(localpath);
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096, false);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                IOUtils.closeStream(fileInputStream);
            }
            if (fsDataOutputStream != null) {
                IOUtils.closeStream(fsDataOutputStream);
            }
        }
    }
}
