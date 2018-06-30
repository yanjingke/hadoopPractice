package com.itacast.weblogwash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WebLogPreProcess {
    static class WebLogPreProcessMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        Text k =new Text();
        NullWritable v =NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            WebLogBen webLogBen = WebLogParser.parser(line);
            if(!webLogBen.isValid()){
                return;
            }
            k.set(webLogBen.toString());
            context.write(k,v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogPreProcess.class);
        job.setMapperClass(WebLogPreProcessMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}
