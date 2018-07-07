package com.itacast.combineFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class SimllFilesToSequenceFileConverter  {


    static class SequenceFileConverterMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
       private Text k;

       @Override
       protected void setup(Context context) throws IOException, InterruptedException {
           InputSplit split = context.getInputSplit();
           Path path = ((FileSplit) split).getPath();
           k=new Text(path.toString());
       }

       @Override
       protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
           context.write(k,value);
       }
   }

    public static void main(String[] args) throws Exception {
        Configuration conf =new Configuration();
        Job job = Job.getInstance(conf, "combin");
        job.setJarByClass(SimllFilesToSequenceFileConverter.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileConverterMapper.class);
        String a=args[0];
        String b=args[1];
        FileInputFormat.setInputPaths(job,new Path(a));
        FileOutputFormat.setOutputPath(job,new Path(b));
        job.waitForCompletion(true);
    }

}
