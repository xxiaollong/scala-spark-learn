package com.example.mr.iformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * KeyValueTextInputFormat：处理KV数据
 *
 */
public class KVText {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 设置切割字符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(KVText.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置数据类型为KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

}

class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        // 封装数据

        // 写出
        context.write(key, v);

    }
}

class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable v = new IntWritable(0);

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
