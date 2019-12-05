package com.example.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 分组排序求最大值
 */
public class OrderSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(OrderSort.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置分组
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);
    }
}

class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();

        // 切分数据
        String[] split = line.split(" ");

        // 封装数据
        k.setAll(Integer.valueOf(split[0]), Double.parseDouble(split[2]));

        // 写出数据
        context.write(k, NullWritable.get());
    }
}

class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}


class OrderGroupComparator extends WritableComparator{

    protected OrderGroupComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        if (aBean.getOrder_id() > bBean.getOrder_id()){
            return 1;
        }else if (aBean.getOrder_id() < bBean.getOrder_id()){
            return -1;
        }else {
            return 0;

        }
    }
}

