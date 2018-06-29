package com.itacast.mr.inverindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InverIndexStepTwo {
    static class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text k=new Text();
        Text v=new Text();
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[]wordandfile=line.split("--");
            k.set(wordandfile[0]);
            v.set(wordandfile[1]);
            context.write(k,v);
        }
    }
    static  class  IndexStepTwoReduce extends Reducer<Text,Text,Text,Text>{
        Text v=new Text();
        @Override

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb=new StringBuffer();
            for(Text value:values){
                sb.append(value.toString());
            }
            v.set(sb.toString());
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf= new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(InverIndexStepTwo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReduce.class);
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
