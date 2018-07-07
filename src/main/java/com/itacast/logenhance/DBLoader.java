package com.itacast.logenhance;



import java.sql.*;

import java.util.Map;
public class DBLoader {

    public static void dbLoader(Map<String,String> ruleMap) throws Exception {
        Connection conn=null;
        Statement st=null;
        ResultSet res=null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn=DriverManager.getConnection("jdbc:mysql://192.168.199.53:3306/urldb?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT","root","123");
            st=conn.createStatement();
            res=st.executeQuery("select url,content from url_rule");
            while (res.next()){
                ruleMap.put(res.getString(1),res.getString(2));

            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (res!=null){
                res.close();
            }
            if (st!=null){
                st.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }

}
