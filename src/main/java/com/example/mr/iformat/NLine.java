package com.example.mr.iformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * NLineInputFormat：按照行数切片
 *
 */
public class NLine {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(NLine.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置切片的行数
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        job.setInputFormatClass(NLineInputFormat.class);


        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}

class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();

        // 切割数据
        String[] split = line.split(" ");

        // 封装数据
        for (String s : split) {
            k.set(s);
            // 写出数据

            context.write(k, v);
        }

    }
}

class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable v = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        v.set(0);

        // 循环累加
        for (IntWritable value : values) {
            v.set(v.get() + value.get());
        }

        // 写出数据
        context.write(key, v);
    }
}

