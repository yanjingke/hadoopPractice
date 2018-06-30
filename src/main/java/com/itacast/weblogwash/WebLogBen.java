package com.itacast.weblogwash;

public class WebLogBen {
    private String remote_addr;//记录客服端的IP地址；
    private String remote_user;//记录客服端的名称，忽略属性"-"
    private String time_local;//记录访问时间与时区
    private String request;//记录请求的URL与HTTP协议
    private String status;//记录请求状态：成功200
    private String boby_bytes_sent;//发送给客服端文件内容主体的大小
    private String http_referer;//用来记录从那个页面链接访问过来的
    private String http_user_agent;//用来记录客服端浏览的相关信息
    private boolean valid=true;//判断数据是否合法


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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBoby_bytes_sent() {
        return boby_bytes_sent;
    }

    public void setBoby_bytes_sent(String boby_bytes_sent) {
        this.boby_bytes_sent = boby_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.valid);
        sb.append("\001").append(remote_addr);
        sb.append("\001").append(remote_user);
        sb.append("\001").append(time_local);
        sb.append("\001").append(request);
        sb.append("\001").append(status);
        sb.append("\001").append(boby_bytes_sent);
        sb.append("\001").append(http_referer);
        sb.append("\001").append(http_user_agent);

        return sb.toString();
    }

}
