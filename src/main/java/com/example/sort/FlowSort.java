package com.example.sort;

import com.example.mr.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 全部排序
 *
 * 分区内排序
 *
 */
public class FlowSort {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(FlowSort.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(FlowBeanSort.class);
        job.setMapOutputValueClass(Text.class);

        // 分区及文件个数
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanSort.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }

}

class FlowSortMapper extends Mapper<LongWritable, Text, FlowBeanSort, Text>{
    FlowBeanSort k = new  FlowBeanSort();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();

        // 切割数据
        String[] split = line.split(" ");

        // 封装数据
        k.setAll(Long.parseLong(split[1]), Long.parseLong(split[2]));
        v.set(split[0]);

        // 写出数据
        context.write(k, v);
    }
}

class FlowSortReducer extends Reducer<FlowBeanSort, Text, Text, FlowBeanSort>{
    @Override
    protected void reduce(FlowBeanSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 循环写出
        for (Text value : values) {
            context.write(value, key);
        }

    }
}

// 自定义分区
class MyPartitioner extends Partitioner<FlowBeanSort, Text> {

    @Override
    public int getPartition(FlowBeanSort flowBeanSort, Text text, int i) {
        String key = text.toString();
        if (key.startsWith("185")){
            return 0;
        }else if (key.startsWith("186")){
            return 1;
        }else if (key.startsWith("135")){
            return 2;
        }else {
            return 3;
        }
    }
}
