package com.itacast.combineFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value=new BytesWritable();
    private boolean processed=false;


    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit=(FileSplit)inputSplit;
        this.conf=taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file=fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in=null;
            try {
                in=fs.open(file);
                IOUtils.readFully(in,contents,0,contents.length);
               value.set(contents,0,contents.length);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(in);
            }
            processed=true;
            return true;

        }
        return false;
    }//通过这个方法给NullWritable k和v赋值

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }//返回K

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }//返回value

    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f :0.0f;
    }

    public void close() throws IOException {

    }
}
