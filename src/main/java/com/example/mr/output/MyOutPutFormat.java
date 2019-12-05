package com.example.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出
 *
 */
public class MyOutPutFormat {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(MyOutPutFormat.class);

        // 关联Mapper和Reducer类
        job.setMapperClass( MyOutPutMapper.class);
        job.setReducerClass( MyOutPutReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义输出类型
        job.setOutputFormatClass(MyFileOutPutFormat.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);


    }

}

class MyOutPutMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 读取数据
        // 封装数据
        k.set(value.toString());

        // 写出数据
        context.write(k, NullWritable.get());
    }
}

class MyOutPutReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        k.set(key.toString() + "\r\n");

        // 循环写出数据
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }

    }
}

// 自定义输出流
class MyFileOutPutFormat extends FileOutputFormat<Text, NullWritable>{

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecordWriter(context);
    }
}

// 输出流的业务逻辑写这里
class MyRecordWriter extends RecordWriter<Text, NullWritable>{
    private FSDataOutputStream fos_a;
    private FSDataOutputStream fos_o;

    public MyRecordWriter(TaskAttemptContext context){
        try {
            FileSystem fs = FileSystem.get(context.getConfiguration());

            fos_a = fs.create(new Path("E:/hadoopTestFile/output/a.txt"));
            fos_o = fs.create(new Path("E:/hadoopTestFile/output/o.txt"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().startsWith("AA")){
            fos_a.write(text.toString().getBytes());
        }else{
            fos_o.write(text.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fos_a);
        IOUtils.closeStream(fos_o);
    }
}
