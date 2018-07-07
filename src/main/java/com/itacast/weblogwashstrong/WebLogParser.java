package com.itacast.weblogwashstrong;

import com.itacast.weblogwash.WebLogBen;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

public class WebLogParser {
    static SimpleDateFormat dt1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
    static SimpleDateFormat dt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static WebLogStrongBen parser(String line){
        WebLogStrongBen webLogBen=new WebLogStrongBen();
            String[]arr=line.split(" ");
            if(arr.length>11){
                webLogBen.setRemote_addr(arr[0]);
                webLogBen.setRemote_user(arr[1]);
                //System.out.println(arr[3].substring(1));
               String time_local= parseTime(arr[3].substring(1));
               if(time_local==null)
               {time_local="-invalid_time-";}
                webLogBen.setTime_local(time_local);

                webLogBen.setRequest(arr[6]);
                webLogBen.setStatus(arr[8]);
                webLogBen.setBody_bytes_sent(arr[9]);
                webLogBen.setHttp_referer(arr[10]);

                if ((arr.length>12)){
                    StringBuffer sb=new StringBuffer();
                    for (int i=11;i<arr.length;i++){
                        sb.append(arr[i]);
                    }
                      webLogBen.setHttp_user_agent(sb.toString());

                }else {
                    webLogBen.setHttp_user_agent(arr[11]);
                }
                if(Integer.parseInt(webLogBen.getStatus())>400){
                    webLogBen.setValid(false);
                }
                if (webLogBen.getTime_local()=="-invalid_time-"){
                    webLogBen.setValid(false);
                }
            }
            else {
                webLogBen.setValid(false);
            }
            return webLogBen;
    }
    public static String parseTime(String dt){

        String timeString="";
        try {
            Date parse = dt1.parse(dt);
            timeString=dt2.format(parse);

        } catch (Exception e) {
            return  null;
        }
        return  timeString;

    }
    public  static void filtStaticResource(WebLogStrongBen webLogBen, Set<String> pages){
        if(!pages.contains(webLogBen.getRequest())){
            webLogBen.setValid(false);
        }
    }
    public static void main(String[] args) {
        WebLogParser webLogParser = new WebLogParser();
       System.out.print( webLogParser.parseTime("18/Sep/2013:06:50:08"));
    }
}
