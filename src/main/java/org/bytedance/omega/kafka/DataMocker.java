package org.bytedance.omega.kafka;

import com.alibaba.fastjson.JSONObject;

public class DataMocker {

    int decisionCounter = 0;

    public String getTpl(){
        String tpl = "{\n" +
                "\t\"basic_info\": {\n" +
                "\t\t\"icode\": \"0000000000\",\n" +
                "\t\t\"req_sn\": \"122332222\",\n" +
                "\t\t\"version\": \"1.0\",\n" +
                "\t\t\"charset\": \"utf-8\",\n" +
                "\t\t\"req_type\": \"sync\",\n" +
                "\t\t\"callback_url\": \"\",\n" +
                "\t\t\"timestamp\": 1535512529,\n" +
                "\t\t\"event_type\": \"pay_result\",\n" +
                "\t\t\"decision_id\": \"1000\",\n" +
                "\t\t\"uid\": \"111\",\n" +
                "\t\t\"ut\": 1,\n" +
                "\t\t\"app_id\": 1,\n" +
                "\t\t\"uuid\": \"\",\n" +
                "\t\t\"time_cost\": \"200\",\n" +
                "\t\t\"ab_test_experiment_id\": \"财经消金-放心借\"\n" +
                "\t},\n" +
                "\t\"biz_info\": {\n" +
                "\t\t\"account_id\": 12353669,\n" +
                "\t\t\"name\": \"王雷\",\n" +
                "\t\t\"id_number\": \"370982198802093418\",\n" +
                "\t\t\"phone\": \"17600649268\",\n" +
                "\t\t\"cardid\": \"370982198802093418\",\n" +
                "\t\t\"idcard_addr\": \"山东省新泰市宫里镇王家隅村246号\",\n" +
                "\t\t\"idcard_start_date\": \"2017-07-10\",\n" +
                "\t\t\"idcard_end_date\": \"2037-07-10\",\n" +
                "\t\t\"bank_name\": \"招商银行\",\n" +
                "\t\t\"bank_id\": \"0308\",\n" +
                "\t\t\"bank_province\": \"北京\",\n" +
                "\t\t\"bank_city\": \"北京\",\n" +
                "\t\t\"company_province_addr\": \"北京\",\n" +
                "\t\t\"company_city_addr\": \"北京\",\n" +
                "\t\t\"company_district_addr\": \"东城区\",\n" +
                "\t\t\"company_addr\": \"东华门街道王府世纪\",\n" +
                "\t\t\"contact_list\": [{\n" +
                "\t\t\t\t\"contact_name\": \"打交道好\",\n" +
                "\t\t\t\t\"contact_phone\": \"18054546669\",\n" +
                "\t\t\t\t\"contact_relation\": 3,\n" +
                "\t\t\t\t\"contactName\": \"打交道好\",\n" +
                "\t\t\t\t\"contactPhone\": \"18054546669\",\n" +
                "\t\t\t\t\"contactRelation\": 3\n" +
                "\t\t\t},\n" +
                "\t\t\t{\n" +
                "\t\t\t\t\"contact_name\": \"很多\",\n" +
                "\t\t\t\t\"contact_phone\": \"13585768886\",\n" +
                "\t\t\t\t\"contact_relation\": 6,\n" +
                "\t\t\t\t\"contactName\": \"很多\",\n" +
                "\t\t\t\t\"contactPhone\": \"13585768886\",\n" +
                "\t\t\t\t\"contactRelation\": 6\n" +
                "\t\t\t}\n" +
                "\t\t],\n" +
                "\t\t\"income_range\": \"4\",\n" +
                "\t\t\"education_background\": \"3\",\n" +
                "\t\t\"credit_check_auth\": 1,\n" +
                "\t\t\"vivo_detect_result\": 200\n" +
                "\t},\n" +
                "\t\"flow_id\": \"放心借-基本信息授信\",\n" +
                "\t\"executed_activity_ids\": [\"身份校验\", \"同盾校验\", \"授信\"],\n" +
                "\t\"activity\": {\n" +
                "\t\t\"身份校验\": {\n" +
                "\t\t\t\"feature\": [\"name\", \"age\"],\n" +
                "\t\t\t\"event\": \"xiaojin_identity_check\",\n" +
                "\t\t\t\"rule\": [84],\n" +
                "\t\t\t\"decision\": {\n" +
                "\t\t\t\t\"decision\": \"deny\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"transition\": [{\n" +
                "\t\t\t\t\"condition\": \"decision=='deny'\",\n" +
                "\t\t\t\t\"next_activity_id\": \"同盾校验\"\n" +
                "\t\t\t}, {\n" +
                "\t\t\t\t\"condition\": \"decision!='deny'\",\n" +
                "\t\t\t\t\"next_activity_id\": \"授信\"\n" +
                "\t\t\t}]\n" +
                "\t\t},\n" +
                "\t\t\"同盾校验\": {\n" +
                "\t\t\t\"feature\": [\"tongdun_score\", \"in_blacklist\"],\n" +
                "\t\t\t\"event\": \"xiaojin_tongdun_check\",\n" +
                "\t\t\t\"decision\": {\n" +
                "\t\t\t\t\"decision\": \"pass\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"transition\": [{\n" +
                "\t\t\t\t\"condition\": \"decision=='pass' && CR>5\",\n" +
                "\t\t\t\t\"next_activity_id\": \"授信\"\n" +
                "\t\t\t}]\n" +
                "\t\t},\n" +
                "\t\t\"授信\": {\n" +
                "\t\t\t\"feature\": [\"CR\"],\n" +
                "\t\t\t\"event\": \"xiaojin_basic_info_credit\",\n" +
                "\t\t\t\"decision\": {\n" +
                "\t\t\t\t\"amount\": 6000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"transition\": []\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"feature\": {\n" +
                "\t\t\"CR\": 6,\n" +
                "\t\t\"age\": 16,\n" +
                "\t\t\"name\": \"张三\",\n" +
                "\t\t\"tongdun_score\": 59,\n" +
                "\t\t\"in_blacklist\": false\n" +
                "\t},\n" +
                "\t\"decision\": {\n" +
                "\t\t\"decision\": \"pass\",\n" +
                "\t\t\"amount\": 6000\n" +
                "\t},\n" +
                "\t\"rule_status\": {\n" +
                "\t\t\"84\": {\n" +
                "\t\t\t\"Hit\": true,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"decision\": \"deny\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"165\": {\n" +
                "\t\t\t\"Hit\": false,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"decision\": \"deny\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"166\": {\n" +
                "\t\t\t\"Hit\": true,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"decision\": \"pass\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"85\": {\n" +
                "\t\t\t\"Hit\": true,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"decision\": \"pass\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"86\": {\n" +
                "\t\t\t\"Hit\": false,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"amount\": 6000\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"87\": {\n" +
                "\t\t\t\"Hit\": true,\n" +
                "\t\t\t\"Operation\": \"SET\",\n" +
                "\t\t\t\"Config\": {\n" +
                "\t\t\t\t\"kv\": {\n" +
                "\t\t\t\t\t\"amount\": 6000\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"risk_response\": {\n" +
                "\t\t\"ret_code\": \"OM0501\",\n" +
                "\t\t\"ret_msg\": \"deny\",\n" +
                "\t\t\"risk_detail\": \"{}\"\n" +
                "\t}\n" +
                "}";

        return tpl;
    }


    public String mockData(){
        JSONObject jjj = JSONObject.parseObject(getTpl());
        String decisionCounter = "" +  ++this.decisionCounter;
        jjj.getJSONObject("basic_info").put("decision_id",decisionCounter);
        return jjj.toJSONString();
    }


    public static void main(String[] args){
        DataMocker dm = new DataMocker();
        System.out.println(dm.mockData());
        System.out.println(dm.mockData());
    }

}
