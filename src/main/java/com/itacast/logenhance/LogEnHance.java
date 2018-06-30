package com.itacast.logenhance;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class LogEnHance {
    static class  logEnHanceMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        Map<String,String>ruleMap=new HashMap<String,String>();
        Text k=new Text();
        NullWritable v=  NullWritable.get();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //从数据库中加载规则信息
            try {
                DBLoader.dbLoader(ruleMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Counter counter=context.getCounter("malformed","malforedline");
            String line=value.toString();
            String[]fields=line.split("\t");
            try {
                String url=fields[28];
                String contain_tag=ruleMap.get(url);
                //判断内容标签是否为空，则只输出url到待爬清单,如果有值则追加
                if (contain_tag==null){
                    k.set(url+"\t"+"tocrawal"+"\n");
                    context.write(k,v);
                }
                else {
                    k.set(line+"\t"+contain_tag+"\n");
                    context.write(k,v);
                }
            } catch (Exception e) {
                counter.increment(1);
            }
        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job=Job.getInstance(conf);
//        job.setJarByClass(LogEnHance.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//        job.setMapperClass(logEnHanceMap.class);
//        job.setOutputFormatClass(LogEnHanceOutputFormat.class);
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//
//        job.setNumReduceTasks(0);
//        job.waitForCompletion(true);
//        System.exit(0);
//    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEnHance.class);

        job.setMapperClass(logEnHanceMap.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
        job.setOutputFormatClass(LogEnHanceOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
        // 在fileoutputformat中，必须输出一个_success文件，所以在此还需要设置输出path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 不需要reducer
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
        System.exit(0);

    }
}
