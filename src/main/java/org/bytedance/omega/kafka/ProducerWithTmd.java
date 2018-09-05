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
    DataMocker dm = new DataMocker();

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

    public  String mockData(){
        String str = this.dm.mockData();
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
                Thread.sleep(100000);

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


