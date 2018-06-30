package com.itacast.logenhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/*
* maptask或者reducetask先调用 OutputFormat的getRecordWriter获得RecordWriter，然后调用RecordWriter把数据写出
* getRecordWrite只调用一次
* */

public class LogEnHanceOutputFormat extends FileOutputFormat<Text,NullWritable> {

    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        //本地测试

//        Path enhancePath =new Path("file:/E:/wc/logenhance.txt");
//        Path tocrawPath =new Path("file:/E:/wc/tocrawl/url.txt");
        Path enhancePath =new Path("hdfs://192.168.199.53:9000/logenhance.txt");
        Path tocrawPath =new Path("hdfs://192.168.199.53:9000/logenhance/tocrawl/url.txt");
        FSDataOutputStream enOutputStream= fs.create(enhancePath);
        FSDataOutputStream tooutputStream = fs.create(tocrawPath);
        return new EnhaceRecoedWriter(enOutputStream,tooutputStream);

    }
    /*构造个自己的recondWriter
    * */
    static class  EnhaceRecoedWriter extends  RecordWriter<Text,NullWritable>{
        FSDataOutputStream enOutputStream=null;
        FSDataOutputStream tooutputStream = null;

        public EnhaceRecoedWriter(FSDataOutputStream enOutputStream, FSDataOutputStream tooutputStream) {
            this.enOutputStream = enOutputStream;
            this.tooutputStream = tooutputStream;
        }

        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String k=text.toString();
            if(k.contains("tocrawal")){
                //如果要写出数据到带爬URL,则写入/logenhance/tocrawl/url.dat
                 tooutputStream.write(k.getBytes());
            }
            else {
                //如果要写出数据到增强日志,则写入/enhancedlog/log.dat
                enOutputStream.write(k.getBytes());
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(tooutputStream!=null){
                tooutputStream.close();
            }
           if(enOutputStream!=null){
                enOutputStream.close();
           }

        }
    }
}
