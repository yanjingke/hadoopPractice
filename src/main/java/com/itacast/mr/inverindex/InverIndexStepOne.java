package com.itacast.mr.inverindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class InverIndexStepOne {
    static class InverIndexStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        Text k=new Text();
        IntWritable v= new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[]words=line.split(" ");
            FileSplit infileSplit= (FileSplit) context.getInputSplit();
            String fileName=infileSplit.getPath().getName();
            for (String word:words){
                k.set(word+"--"+fileName);
                context.write(k,v);
            }
        }
    }
    static  class  InverIndexStepOnereduce extends Reducer<Text,IntWritable,Text,LongWritable>{

        LongWritable v= new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(IntWritable value:values){
                count+=value.get();

            }
            v.set(count);
            context.write(key,v);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(InverIndexStepOne.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
      //  FileOutputFormat.setOutputPath(job.new Path(args[0]));
        job.setMapperClass(InverIndexStepOneMapper.class);
        job.setReducerClass(InverIndexStepOnereduce.class);
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
