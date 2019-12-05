package com.example.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


/**
 *
 * 压缩
 * 解压缩
 *
 */
public class TestCompress {
    public static void main(String[] args) throws Exception {
//        // 压缩为.deflate文件
//        compress("E:/hadoopTestFile/pv.log","org.apache.hadoop.io.compress.DefaultCodec");
//        // 压缩为.gz文件
//        compress("E:/hadoopTestFile/pv.log","org.apache.hadoop.io.compress.GzipCodec");
//        // 压缩为.bz2文件
//        compress("E:/hadoopTestFile/pv.log","org.apache.hadoop.io.compress.BZip2Codec");

        decompress("E:/hadoopTestFile/pv.log.bz2");
    }


    // 压缩
    private static void compress(String fileName, String method) throws Exception {

        // 获取输入流
        FileInputStream inputStream = new FileInputStream(new File(fileName));

        // 根据名称获取解码器/编码器
        Class name = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(name, new Configuration());

        // 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream codecOutputStream = codec.createOutputStream(outputStream);

        // 流的拷贝
        IOUtils.copyBytes(inputStream, codecOutputStream, 1024*1024, false);

        // 关闭资源
        IOUtils.closeStream(codecOutputStream);
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);

    }

    // 解压缩
    private static void decompress(String filename) throws Exception {
        // 合法性检查
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = codecFactory.getCodec(new Path(filename));
        if (codec == null){
            System.out.println("不支持此种文件格式");
            return;
        }

        // 创建压缩文件输入流
        CompressionInputStream codecInputStream = codec.createInputStream(new FileInputStream(filename));

        // 创建解压文件输出流
        FileOutputStream outputStream = new FileOutputStream(filename + ".decode");

        // 流对接
        IOUtils.copyBytes(codecInputStream, outputStream, 1024*1024,false);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(codecInputStream);




    }
}
