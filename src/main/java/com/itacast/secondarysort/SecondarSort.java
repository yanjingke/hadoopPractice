package com.itacast.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecondarSort {
    static class SecondarSortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
        OrderBean bean=new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[]orders=line.split(",");
            bean.set(orders[0],Double.parseDouble(orders[2]));
            context.write(bean,NullWritable.get());
        }
    }
    //因为有groupComparator相同的 所有bean已经被看成一组,且金额最大的那一个排在第一位
    static class SecondarSortReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondarSort.class);
        job.setMapperClass(SecondarSortMapper.class);
        job.setReducerClass(SecondarSortReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(ItemidGroupingComparator.class);//在此设置GroupingComparator类
        job.setPartitionerClass(ItemIdPartitioner.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}
