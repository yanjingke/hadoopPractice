package com.itacast.weblogwash;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.SimpleFormatter;

public class WebLogParser {
    static SimpleDateFormat dt1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
    static SimpleDateFormat dt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static WebLogBen parser(String line){
            WebLogBen webLogBen=new WebLogBen();
            String[]arr=line.split(" ");
            if(arr.length>11){
                webLogBen.setRemote_addr(arr[0]);
                webLogBen.setRemote_user(arr[1]);
                //System.out.println(arr[3].substring(1));
                webLogBen.setTime_local(parseTime(arr[3].substring(1)));

                webLogBen.setRequest(arr[6]);
                webLogBen.setStatus(arr[8]);
                webLogBen.setBoby_bytes_sent(arr[9]);
                webLogBen.setHttp_referer(arr[10]);
                if ((arr.length>12)){
                    webLogBen.setHttp_user_agent(arr[11]+""+arr[12]);

                }else {
                    webLogBen.setHttp_user_agent(arr[11]);
                }
                if(Integer.parseInt(webLogBen.getStatus())>400){
                    webLogBen.setValid(false);
                }else {
                    webLogBen.setValid(true);
                }
            }
            return webLogBen;
    }
    public static String parseTime(String dt){

        String timeString="";
        try {
            Date parse = dt1.parse(dt);
            timeString=dt2.format(parse);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  timeString;

    }

    public static void main(String[] args) {
        WebLogParser webLogParser = new WebLogParser();
       System.out.print( webLogParser.parseTime("18/Sep/2013:06:50:08"));
    }
}
