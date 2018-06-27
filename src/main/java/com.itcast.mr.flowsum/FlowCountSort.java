package com.itcast.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountSort {
    static class  FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
        FlowBean flowBean=new FlowBean();
        Text v=new Text();
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到上一个汇总输出结果,已经是各手机号的总流量信息1
            String line=value.toString();
            String[] fields = line.split("\t");
            String phoneNbr=fields[0];
            long upFlow=Long.parseLong(fields[1]);
            long dFlow=Long.parseLong(fields[2]);
            flowBean.set(upFlow,dFlow);
            v.set(phoneNbr);
            context.write(flowBean,v);
        }
    }
    static class FlowCountSortReduce extends Reducer<FlowBean,Text,Text,FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(),key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(FlowCountSort.class);
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReduce.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
