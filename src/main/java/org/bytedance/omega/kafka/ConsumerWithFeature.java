package org.bytedance.omega.kafka;

import com.bytedance.commons.conf.Conf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ConsumerWithFeature {
    private static Conf conf =
            new Conf("/Users/lijinpeng/app/kafka/ss_conf/kafka.conf");//remote

    private KafkaConsumer<String, byte[]> consumer;

    private Properties props;
    private String kafkaZk;
    private int id;


    /**
     * kafka_test 10.8.128.135:9192,10.8.128.136:9192,10.8.128.137:9192,10.8.128.138:9192,10.8.128.139:9192
     * kafka_test_zookeeper 10.6.128.152:2185,10.6.129.12:2185,10.6.130.76:2185,10.6.130.95:2185,10.6.130.145:2185/kafka-test
     *
     * kafka_risc_lf consul:kafka_risc.service.lf
     * kafka_risc_lf_zookeeper 10.6.128.152:2185,10.6.129.12:2185,10.6.130.76:2185,10.6.130.95:2185,10.6.130.145:2185/kafka-risc
     *z
     * @param consumerId
     */

    public ConsumerWithFeature(int consumerId) {
        props = new Properties();
        kafkaZk = conf.getString("kafka_test");
        id = consumerId;



        //Set by your own kafka server name
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.8.128.135:9192,10.8.128.136:9192,10.8.128.137:9192,10.8.128.138:9192,10.8.128.139:9192");

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



    public static final String TOPIC = "omega_log_monitor_feature_t0";
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

        ConsumerWithFeature csm0 = new ConsumerWithFeature(1);
        csm0.SubscribeTopicPartition(topParts);
        csm0.DealMessage();
    }


    public static void test2(){
        ConsumerWithFeature csm0 = new ConsumerWithFeature(1);
        System.out.println(csm0.consumer.subscription().size());
    }

    public static void main(String[] args){
        test1();
    }



}
