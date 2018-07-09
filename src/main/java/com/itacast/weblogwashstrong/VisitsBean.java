package com.itacast.weblogwashstrong;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VisitsBean implements Writable {
    private boolean valid = true;// 判断数据是否合法
    private String Sessionid;


    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String into_time_local;// 记录访问时间与时区
    private String leave_time_local;// 记录离开时间与时区
    private String into_request;// 进入的url与http协议
    private String leave_request;// 离开的url与http协议
    private int page=0;//访问页面数
    private String http_referer;// 用来记录从那个页面链接访问过来的
    public VisitsBean() {
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

    public String getInto_time_local() {
        return into_time_local;
    }

    public void setInto_time_local(String into_time_local) {
        this.into_time_local = into_time_local;
    }

    public String getLeave_time_local() {
        return leave_time_local;
    }

    public void setLeave_time_local(String leave_time_local) {
        this.leave_time_local = leave_time_local;
    }

    public String getInto_request() {
        return into_request;
    }

    public void setInto_request(String into_request) {
        this.into_request = into_request;
    }

    public String getLeave_request() {
        return leave_request;
    }

    public void setLeave_request(String leave_request) {
        this.leave_request = leave_request;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getSessionid());
        sb.append("\001").append(this.getRemote_addr());
        sb.append("\001").append(this.getInto_time_local());
        sb.append("\001").append(this.getLeave_time_local());
        sb.append("\001").append(this.getInto_request());
        sb.append("\001").append(this.getLeave_request());
        sb.append("\001").append(this.getHttp_referer());
        sb.append("\001").append(this.getPage());
        return sb.toString();
    }
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.valid);
        out.writeUTF(null==Sessionid ?"":Sessionid);
        out.writeUTF(null==remote_addr?"":remote_addr);
        out.writeUTF(null==remote_user?"":remote_user);
        out.writeUTF(null==into_time_local?"":into_time_local);
        out.writeUTF(null==leave_time_local?"":leave_time_local);
        out.writeUTF(null==http_referer?"":http_referer);
        out.writeUTF(null==into_request?"":into_request);
        out.writeUTF(null==leave_request?"":leave_request);
        out.writeInt(page);
    }

    public void readFields(DataInput in) throws IOException {
        this.valid = in.readBoolean();
        this.Sessionid = in.readUTF();
        this.remote_addr = in.readUTF();
        this.remote_user = in.readUTF();
        this.into_time_local = in.readUTF();
        this.leave_time_local = in.readUTF();
        this.http_referer = in.readUTF();
        this.into_request= in.readUTF();
        this.leave_request = in.readUTF();
        this.page=in.readInt();
    }

}
