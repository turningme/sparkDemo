package org.bytedance.omega.kafka;

import com.bytedance.commons.conf.Conf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bytedance.omega.util.JsonUtil;

import java.util.Properties;

public class ProducerWithMockData {
    private static Conf conf =
            new Conf("/Users/lijinpeng/app/kafka/ss_conf/kafka.conf");//remote

    private Properties props;
    private String kafkaZk;
    private KafkaProducer<String, String> producer =null;

    public ProducerWithMockData() {
        props = new Properties();
        kafkaZk = conf.getString("kafka_test_zookeeper");

        //Set by your own kafka server name
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                conf.getServers("kafka_test"));

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


    public void SendMessageWithMock(){
        try {
            for (int i = 1; i < 1100; i++) {
                String msg = JsonUtil.mockData();
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
        new ProducerWithMockData().SendMessageWithMock();
    }
}
