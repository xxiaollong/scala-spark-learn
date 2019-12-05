package com.example.mr.iformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WordCount案例
 * CombineTextInputFormat案例
 * 调整切片大小案例
 * Combine案例
 * map端输出压缩配置
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        args = new String[]{"E:/hadoopTestFile/wcount.txt",
                "E:/hadoopTestFile/wcoutput"};
        Configuration conf = new Configuration();

//        // -----map端输出压缩配置----
//        // 开启压缩
//        conf.setBoolean("mapreduce.map.output.compress", true);
//        // 配置压缩格式
//        conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(WordCount.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        // ----map之后汇总----
//        job.setCombinerClass(WordCountCombiner.class);
//        // 因操作和reduce阶段完全一样，可简写
//        job.setCombinerClass(WordCountReducer.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // -----对最终输出文件进行压缩-----
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩格式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

//        // ----处理小文件，根据实际情况自定义切片大小----
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        // 设置切片的逻辑大小
//        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 128);

//        // ----设置切片大小----
//        // 如果想让切片变小，则设置最大值小于默认block的大小
//        FileInputFormat.setMaxInputSplitSize(job, 1024*64);
//        // 如果想让切片变大，则设置最小值大于默认block的大小
//        FileInputFormat.setMinInputSplitSize(job, 2014*256);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }

}

/**
 * map阶段
 * <p>
 * KEYIN: 输入数据的key的类型
 * VALUEIN: 输入数据的value的类型
 * KEYOUT: 输出数据的key的类型
 * VALUEOUT: 输出数据的value的类型
 */
class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 切分为单词
        String[] words = line.split(" ");

        // 循环输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }

    }
}

/**
 * reduce阶段
 * <p>
 * KEYIN: 输入数据的key的类型
 * VALUEIN: 输入数据的value的类型
 * KEYOUT: 输出数据的key的类型
 * VALUEOUT: 输出数据的value的类型
 */
class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        // 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        // 写出
        context.write(key, v);

    }
}

// 自定义Combiner，在map之后进行聚合
class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        v.set(0);
        // 累加求和
        for (IntWritable value : values) {
            v.set(v.get() + value.get());
        }

        // 写出
        context.write(key, v);
    }
}

