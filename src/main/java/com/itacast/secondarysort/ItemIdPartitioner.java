package com.itacast.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable> {

    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        //
        return (orderBean.getItemid().hashCode()&Integer.MAX_VALUE)% i;
    }
}
