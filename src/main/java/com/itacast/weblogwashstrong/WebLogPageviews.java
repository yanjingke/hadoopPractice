package com.itacast.weblogwashstrong;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WebLogPageviews {

    static class WebLogPageViewsMap extends Mapper<LongWritable, Text, Text, Pageviews> {
        Pageviews pageviews = new Pageviews();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\001");
            pageviews.setValid(new Boolean(splits[0]));
            pageviews.setRemote_addr(splits[1]);
            pageviews.setRemote_user(splits[2]);
            pageviews.setTime_local(splits[3]);
            pageviews.setRequest(splits[4]);
            pageviews.setHttp_referer(splits[7]);
            k.set(splits[1]);
            context.write(k, pageviews);
        }
    }

    static class WebLogPageViewsReduce extends Reducer<Text, Pageviews, Text, NullWritable> {
         Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<Pageviews> values, Context context) throws IOException, InterruptedException {
            // System.out.println("-------------ASDSADAS");
            ArrayList<Pageviews> pageviewsList = new ArrayList<Pageviews>();

            for (Pageviews pageviews : values) {
                Pageviews bean = new Pageviews();
                try {
                    BeanUtils.copyProperties(bean, pageviews);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pageviewsList.add(bean);
            }
            Collections.sort(pageviewsList, new Comparator<Pageviews>() {
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
            int step = 1;
            String sessionid = UUID.randomUUID().toString();
            long difftime=60;
            for (int i = 0; i < pageviewsList.size(); i++) {
                if (1 == pageviewsList.size()) {
                    Pageviews beantem = pageviewsList.get(0);
                    beantem.setSessionid(sessionid);
                    beantem.setStay_time("60");//默认值为60
                    beantem.setStep_number("step" + step);
                    k.set(beantem.toString());
                    context.write(k, v);
                    break;
                }
                if (i == 0) {
                    continue;
                }
                try {
                    difftime = differentData(pageviewsList.get(i).getTime_local(), pageviewsList.get(i - 1).getTime_local());
                    if (difftime/60>30) {

                        Pageviews beantem = pageviewsList.get(i - 1);
                        beantem.setSessionid(sessionid);
                        beantem.setStep_number("step" + step);
                        k.set(beantem.toString());
                        context.write(k, v);
                        sessionid = UUID.randomUUID().toString();
                        step = 1;
                    } else {

                        Pageviews beantem = pageviewsList.get(i - 1);
                        beantem.setSessionid(sessionid);
                        beantem.setStay_time(String.valueOf(difftime));
                        beantem.setStep_number("step" + step);
                        k.set(beantem.toString());
                        context.write(k, v);
                        step++;
                    }
                    if (i == pageviewsList.size()-1) {

                        Pageviews beantem = pageviewsList.get(i);
                        beantem.setSessionid(sessionid);
                        beantem.setStay_time("60");//默认值为60
                        beantem.setStep_number("step" + step);
                        k.set(beantem.toString());
                        context.write(k, v);

                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
        }

      static public Date toData(String timeStr) throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return df.parse(timeStr);
        }

       static public long differentData(String timestr1, String timestr2) throws ParseException {
            Date date1 = toData(timestr1);
            Date date2 = toData(timestr2);
            long diff = ( (date1.getTime() - date2.getTime())/ 1000);
            return diff;
        }

//        public static void main(String[] args) throws ParseException {
//            System.out.println(differentData("2013-09-18 15:32:10","2013-09-18 15:31:10"));
//        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogPageviews.class);
        job.setMapperClass(WebLogPageViewsMap.class);
        job.setReducerClass(WebLogPageViewsReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pageviews.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("/data/weblog/preprocess/output"));
//        FileOutputFormat.setOutputPath(job, new Path("/data/weblog/preprocess/vaild_output"));


        job.waitForCompletion(true);

    }
}
