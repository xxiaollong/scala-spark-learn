package com.example.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 *  reduce join 案例
 *
 */
public class ReducerJoin {
    public static void main(String[] args) throws Exception {
        args = new String[]{"E:/hadoopTestFile/join", "E:/hadoopTestFile/joinout"};
        Configuration conf = new Configuration();

        // 获取Job对象
        Job job = Job.getInstance(conf);

        // 设置jar存放位置
        job.setJarByClass(ReducerJoin.class);

        // 关联Mapper和Reducer类
        job.setMapperClass( ReducerJoinMapper.class);
        job.setReducerClass( ReducerJoinReducer.class);

        // 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}

class ReducerJoinMapper extends Mapper<LongWritable, Text, Text, TableBean>{
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       FileSplit inputSplit  = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取数据
        String line = value.toString();
        String[] split = line.split(" ");

        // 封装数据
        if (fileName.startsWith("order")){
            k.set(split[1]);
            v.setAll(split[0],split[1],Integer.valueOf(split[2]),"-","order");
        }else{
            k.set(split[0]);
            v.setAll("-",split[0],0,split[1],"product");
        }

        // 写出数据
        context.write(k, v);

    }
}

// join在这里
class ReducerJoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orders = new ArrayList<>();
        TableBean product = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())){
                TableBean bean = new TableBean();
                BeanUtils.copyProperties(value, bean);
                orders.add(bean);
            }else {
                BeanUtils.copyProperties(value, product);
            }
        }

        for (TableBean order : orders) {
            order.setPname(product.getPname());

            // 写出数据
            context.write(order, NullWritable.get());
        }


    }
}
