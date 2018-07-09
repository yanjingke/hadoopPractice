package com.itacast.weblogwashstrong;

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

import java.util.HashSet;
import java.util.Set;

public class WebLogStrong {
    static class WeblogStrongMap extends Mapper<LongWritable, Text, Text, NullWritable>{
        Text k =new Text();
        NullWritable v =NullWritable.get();
        Set<String> pages = new HashSet<String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            WebLogStrongBen  webLogBen = WebLogParser.parser(line);
            if (webLogBen.getRemote_addr()==""||webLogBen.getRemote_addr()==null)
                return;
            WebLogParser.filtStaticResource(webLogBen,pages);
            k.set(webLogBen.toString());
            context.write(k,v);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogStrong.class);
        job.setMapperClass( WeblogStrongMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("/data/weblog/preprocess/input"));
//        FileOutputFormat.setOutputPath(job,new Path("/data/weblog/preprocess/output"));
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
