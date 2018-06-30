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
import java.lang.reflect.Array;
import java.util.Arrays;

public class shareFriendsStepTwo {
    static class shareFriendsStepTwoMapper extends Mapper<LongWritable ,Text,Text,Text>{
        Text k=new Text();
        Text v =new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String  line=value.toString();
            String []friend_persons=line.split("\t");
            String friend=friend_persons[0];
            String[] persons=friend_persons[1].split(",");
            v.set(friend);
            Arrays.sort(persons);
            for(int i=0;i<persons.length;i++){
                for(int j=i+1;j<persons.length;j++){
                    k.set(persons[i]+"-"+persons[j]);
                    context.write(k,v);
                }
            }

        }
    }
    static class shareFriendsStepTwoReduce extends Reducer<Text,Text,Text,Text>{

        Text v=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb=new StringBuffer();
            for(Text persoon:values){
             sb.append(persoon).append(" ");
            }
            v.set(sb.toString());
            context.write(key,v);
        }



    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf);
        job.setJarByClass(shareFriendsStepTwo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]) );
        job.setMapperClass(shareFriendsStepTwoMapper.class);
        job.setReducerClass(shareFriendsStepTwoReduce.class);
        job.waitForCompletion(true);
    }
}
