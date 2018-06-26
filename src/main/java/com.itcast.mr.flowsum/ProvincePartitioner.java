package com.itcast.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    public static HashMap<String,Integer>proviceDict=new HashMap<String, Integer>();
   static {
        proviceDict.put("139",0);
       proviceDict.put("137",1);
       proviceDict.put("138",2);
       proviceDict.put("136",3);
   }
    public int getPartition(Text text, FlowBean flowBean, int i) {
      String prefix= text.toString().substring(0,3);
      Integer provinceId=proviceDict.get(prefix);
      return provinceId==null?4:provinceId;
    }
}
