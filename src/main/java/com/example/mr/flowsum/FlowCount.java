package com.example.mr.flowsum;

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
 *
 *
 * 手机号流量统计
 *
 * 自定义分区：实现手机号按照号段分区
 */
public class FlowCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(FlowCount.class);

        // 关联Mapper和Reducer类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置分区
        job.setPartitionerClass(MyPartitioner.class);
        // 设置reduceTask的个数
        job.setNumReduceTasks(4);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}

class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();

        // 切割数据
        String[] split = line.split(" ");

        // 封装对象
        k.set(split[0]);
        v.setAll(Long.valueOf(split[1]),Long.valueOf(split[2]));

        // 输出
        context.write(k, v);
    }
}

class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

    FlowBean v = new FlowBean(0, 0);

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        v.setAll(0, 0);

        // 累加求和
        for (FlowBean value : values) {
            v.addAll(value.getUpFlow(), value.getDownFlow());
        }

        // 写出
        context.write(key, v);

    }
}

// 自定义分区
class MyPartitioner extends Partitioner<Text, FlowBean>{

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
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
