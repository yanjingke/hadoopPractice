package com.itacast.weblogwashstrong;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pageviews implements Writable {
    private boolean valid = true;// 判断数据是否合法
    private String Sessionid;
    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String stay_time;// 停留时长（单位秒）
    private String step_number;//第几步

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getSessionid() {
        return Sessionid;
    }

    public void setSessionid(String sessionid) {
        Sessionid = sessionid;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStay_time() {
        return stay_time;
    }

    public void setStay_time(String stay_time) {
        this.stay_time = stay_time;
    }

    public String getStep_number() {
        return step_number;
    }

    public void setStep_number(String step_number) {
        this.step_number = step_number;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append("\001").append(this.getSessionid());
        sb.append("\001").append(this.getRemote_addr());
        sb.append("\001").append(this.getTime_local());
        sb.append("\001").append(this.getRequest());
        sb.append("\001").append(this.getStep_number());
        sb.append("\001").append(this.getStay_time());
        sb.append("\001").append(this.getHttp_referer());
        sb.append("\001").append(this.getRemote_user());
        return sb.toString();
    }

    public Pageviews() {
    }

    public void set(boolean valid, String sessionid, String remote_addr, String remote_user, String time_local, String request, String stay_time, String step_number) {
        this.valid = valid;
        this.Sessionid = sessionid;
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.stay_time = stay_time;
        this.step_number = step_number;
        this.http_referer=http_referer;
    }


    public int compareTo(Pageviews o) {
        int cmp=this.remote_addr.compareTo(o.remote_addr);
        return cmp;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.valid);
        out.writeUTF(null==Sessionid ?"":Sessionid);
        out.writeUTF(null==remote_addr?"":remote_addr);
        out.writeUTF(null==remote_user?"":remote_user);
        out.writeUTF(null==time_local?"":time_local);
        out.writeUTF(null==request?"":request);
        out.writeUTF(null==http_referer?"":http_referer);
        out.writeUTF(null==stay_time?"":stay_time);
        out.writeUTF(null==step_number?"":step_number);
    }

    public void readFields(DataInput in) throws IOException {
        this.valid = in.readBoolean();
        this.Sessionid = in.readUTF();
        this.remote_addr = in.readUTF();
        this.remote_user = in.readUTF();
        this.time_local = in.readUTF();
        this.request = in.readUTF();
        this.http_referer = in.readUTF();
        this.stay_time = in.readUTF();
        this.step_number = in.readUTF();
    }
}
