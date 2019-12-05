package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS IO流操作
 * <p>
 * 注意：
 * 上传时：fs负责写出流
 * 下载时：fs负责读入流
 */
public class HDFSIO {
    public static void main(String[] args) throws Exception {
        // 写入
//        writeToHDFS();

        // 读出
//        readFromHDFS();

        // 定位读取
        readFromHDFSSeek();

    }

    // 向HDFS写入文件
    public static void writeToHDFS() throws Exception {
        // 创建配置信息
        Configuration cong = new Configuration();
        // 有用户权限
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), cong, "admin");


        // 获取输入流
        FileInputStream inputStream = new FileInputStream(new File("E:/hadoopTestFile/pv.log"));

        // 获取写出流(不能是目录)
        FSDataOutputStream outputStream = fs.create(new Path("/1101/wxl/pv.log"));

        // 流对接
        IOUtils.copyBytes(inputStream, outputStream, cong);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over..");
    }

    // 从HDFS读出文件
    public static void readFromHDFS() throws Exception {
        // 创建配置信息
        Configuration cong = new Configuration();
        // 有用户权限
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), cong, "admin");

        // 获取读入流(不能是目录)
        FSDataInputStream inputStream = fs.open(new Path("/1101/wxl/pv.log"));

        // 获取写出流
        FileOutputStream outputStream = new FileOutputStream(new File("E:/hadoopTestFile/pv_HDFS.log"));

        // 流对接
        IOUtils.copyBytes(inputStream, outputStream, cong);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over..");
    }

    // 从HDFS定位读取
    public static void readFromHDFSSeek() throws Exception{
        // 创建配置信息
        Configuration cong = new Configuration();
        // 有用户权限
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), cong, "admin");

        // 获取读入流(不能是目录)
        FSDataInputStream inputStream = fs.open(new Path("/se.csv"));

        // 获取写出流
        FileOutputStream outputStream = new FileOutputStream(new File("E:/hadoopTestFile/seek2.csv"));

        // 声明读取偏移量和长度
        long offset = 0;
        long length = 0;

        // 获取文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/se.csv"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            // 指定读取那个数据块
            BlockLocation block = blockLocations[blockLocations.length - 1];
            offset = block.getOffset();
            length = block.getLength();
        }

        // 设置读入偏移量
        inputStream.seek(offset);

        // 流对接
        IOUtils.copyBytes(inputStream, outputStream, length, true);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over..");
    }

}
