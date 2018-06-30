package com.itacast.secondarysort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//public class OrderBean  implements WritableComparable<OrderBean> {
//    private Text itemid;
//    private DoubleWritable amount;
//    public Text getItemid() {
//        return itemid;
//    }
//
//    public void setItemid(Text itemid) {
//        this.itemid = itemid;
//    }
//
//    public DoubleWritable getAmount() {
//        return amount;
//    }
//
//    public void setAmount(DoubleWritable amount) {
//        this.amount = amount;
//    }
//
//    public OrderBean() {
//
//    }
//
//    public OrderBean(Text itemid, DoubleWritable amount) {
//        this.itemid = itemid;
//        this.amount = amount;
//    }
//    public void set(Text itemid, DoubleWritable amount) {
//        this.itemid = itemid;
//        this.amount = amount;
//    }
//    public int compareTo(OrderBean o) {
//        int cmp=this.itemid.compareTo(o.getItemid());
//        if(cmp==0){
//            cmp=-this.amount.compareTo(o.getAmount());
//        }
//        return cmp ;
//    }
//
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeUTF(itemid.toString());
//        dataOutput.writeDouble(amount.get());
//    }
//
//
//    public void readFields(DataInput dataInput) throws IOException {
//        String readUTF = dataInput.readUTF();
//        double readDouble = dataInput.readDouble();
//
//        this.itemid = new Text(readUTF);
//        this.amount= new DoubleWritable(readDouble);
//    }
//
//    @Override
//    public String toString() {
//        return "OrderBean" +
//                "itemid=" + itemid +
//                ", amount=" + amount ;
//    }
//}
public class OrderBean  implements WritableComparable<OrderBean> {
    private String itemid;
    private double amount;

    public OrderBean(String itemid, double amount) {
        this.itemid = itemid;
        this.amount = amount;
    }
    public void set(String itemid, double amount) {
        this.itemid = itemid;
        this.amount = amount;
    }

    public OrderBean() {

    }

    public String getItemid() {
        return itemid;
    }

    public void setItemid(String itemid) {
        this.itemid = itemid;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public int compareTo(OrderBean o) {
        int cmp=this.itemid.compareTo(o.getItemid());
        if(cmp==0){
            cmp=-Double.toString(this.amount).compareTo(Double.toString(o.getAmount()));
        }
        return cmp ;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(itemid);
        dataOutput.writeDouble(amount);
    }


    public void readFields(DataInput dataInput) throws IOException {
        this.itemid = dataInput.readUTF();
        this.amount = dataInput.readDouble();


    }

    @Override
    public String toString() {
        return "OrderBean" +
                "itemid=" + itemid +
                ", amount=" + amount ;
    }
}