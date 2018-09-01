package org.bytedance.omega.kafka;

import com.bytedance.commons.conf.Conf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bytedance.omega.util.JsonUtil;

import java.util.Properties;

public class ProducerWithTmd {
    private static Conf conf =
            new Conf("/Users/lijinpeng/app/kafka/ss_conf/kafka.conf");//remote

    private Properties props;
    private String kafkaZk;
    private KafkaProducer<String, String> producer =null;

    public ProducerWithTmd() {
        props = new Properties();
        kafkaZk = conf.getString("kafka_risc_lf_zookeeper");

        //Set by your own kafka server name
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.10.150.213:9097 10.10.150.214:9097 10.10.150.216:9097 10.10.150.217:9097 10.10.150.41:9097 10.10.150.42:9097 10.10.150.44:9097 10.10.150.45:9097 10.10.150.47:9097 10.10.150.48:9097 10.10.150.80:9097 10.10.150.81:9097 10.10.150.89:9097 10.10.151.158:9097 10.10.152.21:9097 10.10.168.132:9097 10.10.168.135:9097 10.10.168.195:9097 10.10.168.198:9097 10.10.168.201:9097 10.10.168.204:9097 10.10.168.207:9097 10.10.168.210:9097 10.10.168.213:9097 10.10.168.216:9097 10.10.168.219:9097 10.10.168.222:9097 10.10.185.148:9097 10.10.185.207:9097 10.10.190.205:9097 10.11.157.220:9097 10.11.157.221:9097 10.11.157.222:9097 10.11.157.223:9097 10.11.157.224:9097 10.11.157.225:9097 10.11.157.226:9097 10.11.157.227:9097 10.11.157.228:9097 10.11.157.229:9097 10.12.154.135:9097 10.12.154.136:9097 10.12.154.137:9097 10.12.154.138:9097 10.12.154.139:9097 10.12.154.140:9097 10.12.154.141:9097 10.12.154.142:9097 10.12.154.143:9097 10.12.154.144:9097");

        //Set it all (-1 equivalence) to get best consistence
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        //Set a suitable value to get a better performance
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        //Set a suitable value to get a better performance
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    /*public void SendMessage(String topic, String contents)*/

    public void SendMessage(){
        try {
            for (int i = 1000; i < 2006; i++) {
                String msg = "Message " + i;
                producer.send(new ProducerRecord<String, String>
                        ("omega_feature_test",msg));
                System.out.println("Sent:" + msg);
                Thread.sleep(10000);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static String mockData(){
        String str = "{\n" +
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
        return str;
    }


    public void SendMessageWithMock(){
        try {
            for (int i = 1; i < 1100; i++) {
                //String msg = JsonUtil.mockData();
                String msg = mockData();
                producer.send(new ProducerRecord<String, String>
                        ("omega_feature_test",msg));
                System.out.println("Sent:" + msg);
                Thread.sleep(10000);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args){
        new ProducerWithTmd().SendMessageWithMock();
    }
}
