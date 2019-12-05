package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用API的方式操作HDFS
 */
public class HDFSClient {

    public static void main(String[] args) throws Exception {
        // 创建配置信息
        Configuration cong = new Configuration();
        // 默认副本数3
        cong.set("dfs.replication","1");

        // 无用户权限
//        cong.set("fs.default.name", "hdfs://localhost:9000");
        // 获取HDFS客户端对象
//        FileSystem fs = FileSystem.get(cong);

        // 有用户权限
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), cong, "admin");

        //1 在HDFS创建路径
//        fs.mkdirs(new Path("/1101/wxl_1"));

        //2 从本地上传文件到HDFS（默认副本数3）
//        fs.copyFromLocalFile(new Path("E:/hadoopTestFile/wcount.txt"), new Path("/1101/wxl/wcount.txt"));

        //2.1 从本地上传文件到HDFS（设置副本数为2）
//        fs.copyFromLocalFile(new Path("E:/hadoopTestFile/wcount.txt"), new Path("/1101/wxl/wcount_2.txt"));

        //3 修改HDFS文件副本数
//        fs.setReplication(new Path("/1101/wxl/wcount.txt"), (short) 1);

        //4 从HDFS下载文件到本地
//        fs.copyToLocalFile(new Path("/1101/wxl/wcount.txt"), new Path("E:/hadoopTestFile/wcountFromHDFS.txt"));

        //5 删除HDFS文件(参数2需要设置为true或false都行)
//        fs.delete(new Path("/1101/wxl/wcount_2.txt"), true);
        //5 删除HDFS目录(参数2需要设置为true)
//        fs.delete(new Path("/1101/wxl"), true);

        //6 HDFS文件重命名(可修改路径)
//        fs.rename(new Path("/1101/wxl/wcount.txt"), new Path("/1101/wxl/wcount_new.txt"));
//        fs.rename(new Path("/1101/wxl/wcount_new.txt"), new Path("/1101/wcount.txt"));

        //7 查看HDFS文件及目录信息(目录则参数2需要设置为true)
//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/1101"), true);
//        while (listFiles.hasNext()){
//            LocatedFileStatus fileStatus = listFiles.next();
//            System.out.println("路径：" + fileStatus.getPath());
//            System.out.println("权限：" + fileStatus.getPermission());
//            System.out.println("长度：" + fileStatus.getLen());
//
//            System.out.println("============================");
//        }

        //8 HDFS文件及目录的判断
        FileStatus[] listStatus = fs.listStatus(new Path("/1101"));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()){    //文件
                System.out.println("f:" + fileStatus.getPath().getName());
            }else{  //目录
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }


        // 关闭资源
        fs.close();

        System.out.println("over..");
    }


}
