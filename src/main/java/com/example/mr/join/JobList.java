package com.example.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 多个Job串联执行
 *
 */
public class JobList {
    public static void main(String[] args) throws Exception {
        job1();
        job2();
    }

    public static void job2() throws Exception {
        String[] args = new String[]{
                "E:/hadoopTestFile/job1/part-r-00000",
                "E:/hadoopTestFile/job2"
        };

        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(JobList.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(TwoJobMapper.class);
        job.setReducerClass(TwoJobRecuder.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

    public static void job1() throws Exception{
        String[] args = new String[]{
                "E:/hadoopTestFile/myinput/",
                "E:/hadoopTestFile/job1"
        };

        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(JobList.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(OneJobMapper.class);
        job.setReducerClass(OneJobReducer.class);

        // map后汇总
        job.setCombinerClass(OneJobReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}




class OneJobMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit splits = (FileSplit) context.getInputSplit();
        fileName = splits.getPath().getName();
    }

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 读取切割数据
        String[] split = value.toString().split(" ");

        // 封装数据
        for (String s : split) {
            k.set(s +"--"+ fileName);

            // 写出数据
            context.write(k, v);
        }

    }
}

class OneJobReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable v = new IntWritable(0);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        v.set(0);

        // 累加求和
        for (IntWritable value : values) {
            v.set(value.get() + v.get());
        }

        // 写出数据
        context.write(key, v);

    }
}

class TwoJobMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取切割数据
        String[] split = value.toString().split("--");

        // 封装数据
        k.set(split[0]);
        v.set(split[1]);

        // 写出数据
        context.write(k, v);

    }
}

class TwoJobRecuder extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 累加数据
        StringBuffer str = new StringBuffer();
        for (Text value : values) {
            str.append("\t" + value);
        }

        // 写出数据
        context.write(key, new Text(str.toString()));

    }
}

