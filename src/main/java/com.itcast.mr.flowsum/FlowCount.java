package com.itcast.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
;
import java.io.IOException;
import java.util.Iterator;

public class FlowCount {
    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] fields=line.split("\t");
            String phoneNbr=fields[1];
            //
            long upFlow=Long.parseLong(fields[fields.length-3]);
            long dFlow=Long.parseLong(fields[fields.length-2]);
            context.write(new Text(phoneNbr),new FlowBean(upFlow,dFlow));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration cof=new Configuration();
        Job job=Job.getInstance(cof);
        job.setJarByClass(FlowCount.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //指定自定义分区数目
        job.setPartitionerClass(ProvincePartitioner.class);
        //指定reduce数目
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
       boolean res= job.waitForCompletion(true);
       System.exit(res?0:1);
    }
    static class FlowCountReducer  extends Reducer<Text,FlowBean,Text,FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow=0;
            long sum_dFlow=0;
            for (FlowBean bean: values){
                sum_dFlow=bean.getdFlow();
                sum_upFlow=bean.getUpFlow();

            }
            FlowBean flowBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key,flowBean);

        }
    }
   //com.itcast.mr.flowsum.FlowCount.java
}
