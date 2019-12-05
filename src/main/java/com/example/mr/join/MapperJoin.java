package com.example.mr.join;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * mapper join 案例
 *
 * 计数器
 *
 * 数据清洗
 */
public class MapperJoin {
    public static void main(String[] args) throws Exception {
        args = new String[]{"E:/hadoopTestFile/join/order.txt", "E:/hadoopTestFile/mapjoin"};
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(MapperJoin.class);

        // 加载缓存数据
        job.addCacheFile(new URI("file:///E:/hadoopTestFile/join/product.txt"));

        // 关联Mapper和Reducer类
        job.setMapperClass(MapperJoinMapper.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduceTask个数为0,即无reduce阶段
        job.setNumReduceTasks(0);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}

class MapperJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    // 缓存文件
    private HashMap<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(files[0].getPath()),"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] split = line.split(" ");
            pdMap.put(split[0], split[1]);
        }

        IOUtils.closeStream(reader);
    }

    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取切割数据
        String[] split = value.toString().split(" ");

        // 校验数据，数据清洗
        boolean flag = checkId(split[0], context);
        if (!flag){
            return;
        }

        // 封装数据
        k.set(split[0] + "\t" + split[2] + "\t" + pdMap.get(split[1]));

        // 写出数据
        context.write(k, NullWritable.get());
    }

    // 数据清洗
    private boolean checkId(String s, Context context) {

        if (s.startsWith("2")){
            // 设置计数器
            context.getCounter("mapJoin", "true").increment(1);

            return false;
        }else {
            // 设置计数器
            context.getCounter("mapJoin", "false").increment(1);

            return true;
        }

    }
}

