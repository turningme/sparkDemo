package org.bytedance.omega.kafka;

import com.bytedance.commons.conf.Conf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerWithTmd {
    private static Conf conf =
            new Conf("/Users/lijinpeng/app/kafka/ss_conf/kafka.conf");//remote

    private KafkaConsumer<String, byte[]> consumer;

    private Properties props;
    private String kafkaZk;
    private int id;

    public ConsumerWithTmd(int consumerId) {
        props = new Properties();
        kafkaZk = conf.getString("kafka_risc_lf_zookeeper");
        id = consumerId;

        //Set by your own kafka server name
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.10.150.213:9097 10.10.150.214:9097 10.10.150.216:9097 10.10.150.217:9097 10.10.150.41:9097 10.10.150.42:9097 10.10.150.44:9097 10.10.150.45:9097 10.10.150.47:9097 10.10.150.48:9097 10.10.150.80:9097 10.10.150.81:9097 10.10.150.89:9097 10.10.151.158:9097 10.10.152.21:9097 10.10.168.132:9097 10.10.168.135:9097 10.10.168.195:9097 10.10.168.198:9097 10.10.168.201:9097 10.10.168.204:9097 10.10.168.207:9097 10.10.168.210:9097 10.10.168.213:9097 10.10.168.216:9097 10.10.168.219:9097 10.10.168.222:9097 10.10.185.148:9097 10.10.185.207:9097 10.10.190.205:9097 10.11.157.220:9097 10.11.157.221:9097 10.11.157.222:9097 10.11.157.223:9097 10.11.157.224:9097 10.11.157.225:9097 10.11.157.226:9097 10.11.157.227:9097 10.11.157.228:9097 10.11.157.229:9097 10.12.154.135:9097 10.12.154.136:9097 10.12.154.137:9097 10.12.154.138:9097 10.12.154.139:9097 10.12.154.140:9097 10.12.154.141:9097 10.12.154.142:9097 10.12.154.143:9097 10.12.154.144:9097");

        //earliest, latest, none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Set by your own kafka consumer groupx
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_xzx_07");

        //Commit automatically
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //or manually(better for store offset locally)

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, byte[]>(props);
    }

    //subscribe your topics and kafka will do partition-load-balance automatically
    public void SubscribeTopic(Collection<String> subscribeList) {
        consumer.subscribe(subscribeList);
    }

    //control partition-load-balance manually
    public void SubscribeTopicPartition(Collection<TopicPartition> topicPartitions) {
        consumer.assign(topicPartitions);
    }

    public void DealMessage() {
        try {
            ArrayList<TopicPartition> tp = new ArrayList<TopicPartition>();
            //consumer.seek(new TopicPartition("omega_feature_test",0),0);

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
                /* //Deal message by partition
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                        System.out.printf("[ID:%d] offset = %d, key = %s, value = %s%n",
                                id, record.offset(), record.key(), record.value());
                    }
                }
                */

                //Deal message sequentially
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.printf("[ID:%d] partition= %d offset = %d, key = %s, value = %s%n",
                            id,record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }



    public static final String TOPIC = "omega_feature_test";
    public static  List<TopicPartition> mockTopics(){
        List<TopicPartition> topParts = new ArrayList();
        for(int i=0; i<10 ;i++){
            topParts.add(new TopicPartition(TOPIC,i));
        }
        return topParts;
    }

    public static void test1(){
        /*
        
         */

        List<TopicPartition> topParts = mockTopics();

        ConsumerWithTmd csm0 = new ConsumerWithTmd(1);
        csm0.SubscribeTopicPartition(topParts);
        csm0.DealMessage();
    }


    public static void test2(){
        ConsumerWithTmd csm0 = new ConsumerWithTmd(1);
        System.out.println(csm0.consumer.subscription().size());
    }

    public static void main(String[] args){
        test1();
    }



}
