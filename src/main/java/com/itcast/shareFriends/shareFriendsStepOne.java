package com.itcast.shareFriends;

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

public class shareFriendsStepOne {
    static class ShareFriendsFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{
       Text k= new Text();
        Text v= new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[]person_fiends=line.split(":");
            String person=person_fiends[0];
            for (String friend:person_fiends[1].split(",")){
                k.set(friend);
                v.set(person);
                context.write(k,v);
            }
        }
    }
    static  class ShareFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{
        Text v= new Text();
        @Override

        protected void reduce(Text key, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for (Text person:persons){
                stringBuffer.append(person).append(",");
            }
            v.set(stringBuffer.toString());
          context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf);
        job.setJarByClass(shareFriendsStepOne.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapperClass(ShareFriendsFriendsStepOneMapper.class);
        job.setReducerClass(ShareFriendsStepOneReducer.class);
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
