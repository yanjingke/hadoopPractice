package com.itcast.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean  implements Writable {
    private long upFlow;
    private long dFlow;
    private long sumFlow;
    public FlowBean() {
    }

    public FlowBean(long upFlow, long dFlow) {

        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow=upFlow+dFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;

    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        upFlow=dataInput.readLong();
        dFlow=dataInput.readLong();
        sumFlow=dataInput.readLong();
    }
    @Override
    public String toString() {
        return  upFlow+"\t"+dFlow+"\t"+sumFlow;
    }
}
