package com.example.mr.iformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 自定义InputFormat
 * <p>
 * 实现小文件合并功能
 */

class MyInputFormatDriver{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(MyInputFormatDriver.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(MyInputFormatMapper.class);
        job.setReducerClass(MyInputFormatReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输入输出类型
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}

class MyInputFormatMapper extends Mapper<Text, BytesWritable,Text, BytesWritable>{
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}

class MyInputFormatReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value : values) {
            context.write(key, value);
        }
    }
}



public class MyInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordReader recordReader = new MyRecordReader();

        return recordReader;
    }
}

class MyRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit split;
    private Configuration configuration;
    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean isContinue = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isContinue) {
            // 创建缓冲区
            byte[] buf = new byte[(int) split.getLength()];

            // fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);

            // 获取输入流
            FSDataInputStream inputStream = fs.open(path);

            // 拷贝数据
            IOUtils.readFully(inputStream, buf, 0, buf.length);

            // 封装输出
            v.set(buf, 0, buf.length);
            k.set(path.toString());

            // 关闭资源
            IOUtils.closeStream(inputStream);

            isContinue = false;
            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
