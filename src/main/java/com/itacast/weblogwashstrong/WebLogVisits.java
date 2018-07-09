package com.itacast.weblogwashstrong;

import org.apache.commons.beanutils.BeanUtils;
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
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WebLogVisits {
        static  class  WeblogVisitsMap extends Mapper<LongWritable,Text,Text,Pageviews>{
            Pageviews bean=new Pageviews();
            Text k=new Text();
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line =value.toString();
                String[] split = line.split("\001");
                bean.setValid(new Boolean(split[0]));
                bean.setSessionid(split[1]);
                bean.setRemote_addr(split[2]);
                bean.setRemote_user(split[8]);
                bean.setTime_local(split[3]);
                bean.setRequest(split[4]);
                bean.setHttp_referer(split[7]);
                bean.setStep_number(split[5]);
                k.set(split[1]);
                context.write(k,bean);
            }
        }
        static  class  WeblogVisitsReduce extends Reducer<Text,Pageviews,Text,NullWritable>{
           VisitsBean k=new VisitsBean();
           Text text=new Text();
            NullWritable v= NullWritable.get();
            @Override
            protected void reduce(Text key, Iterable<Pageviews> values, Context context) throws IOException, InterruptedException {
                ArrayList<Pageviews> pageviews=new ArrayList<Pageviews>();
                for (Pageviews pageview:values){
                    Pageviews bean = new Pageviews();
                    try {
                        BeanUtils.copyProperties(bean,pageview);
                        pageviews.add(bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Collections.sort(pageviews, new Comparator<Pageviews>() {
                        public int compare(Pageviews o1, Pageviews o2) {
                            try {
                                Date date1 = toData(o1.getTime_local());
                                Date date2 = toData(o2.getTime_local());
                                if (date1 == null || date1 == null)
                                    return 0;
                                return date1.compareTo(date2);
                            } catch (ParseException e) {
                                e.printStackTrace();
                                return 0;
                            }


                        }
                    });

                }
                Pageviews beant= pageviews.get(0);
                Pageviews beantend= pageviews.get(pageviews.size()-1);
                k.setValid(beant.isValid());
                k.setHttp_referer(beant.getHttp_referer());
                k.setRemote_addr(beant.getRemote_addr());
                k.setRemote_user(beant.getRemote_user());
                k.setSessionid(beant.getSessionid());
                k.setInto_request(beant.getRequest());
                k.setLeave_request(beantend.getRequest());
                k.setInto_time_local(beant.getTime_local());
                k.setLeave_time_local(beantend.getTime_local());
                k.setPage(pageviews.size());
                text.set(k.toString());
                context.write(text,v);
            }
            static public Date toData(String timeStr) throws ParseException {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
                return df.parse(timeStr);
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogVisits.class);
        job.setMapperClass(WeblogVisitsMap.class);
        job.setReducerClass(WeblogVisitsReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pageviews.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job, new Path( "/data/weblog/preprocess/vaild_output"));
//
//        FileOutputFormat.setOutputPath(job, new Path( "/data/weblog/preprocess/visits_output"));
        job.waitForCompletion(true);
    }
}