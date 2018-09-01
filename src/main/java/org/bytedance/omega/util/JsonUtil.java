package org.bytedance.omega.util;

import com.alibaba.fastjson.JSONObject;

public class JsonUtil {
    public static String mockData(){
        JSONObject jObj = new JSONObject();
        jObj.put("id","001");
        jObj.put("name","lkp");

        JSONObject subObj = new JSONObject();
        subObj.put("prop1","p1");
        subObj.put("prop2","p2");

        jObj.put("params",subObj);

        return jObj.toJSONString();
    }

    public static void main(String[] args){
        System.out.println(String.format("%s %s","  " ,mockData()));
    }
}
