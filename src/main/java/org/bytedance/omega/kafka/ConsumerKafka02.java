package org.bytedance.omega.kafka;

import com.bytedance.commons.conf.Conf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ConsumerKafka02 {
    private static Conf conf =
            new Conf("/Users/lijinpeng/app/kafka/ss_conf/kafka.conf");//remote

    private KafkaConsumer<String, byte[]> consumer;

    private Properties props;
    private String kafkaZk;
    private int id;

    public ConsumerKafka02(int consumerId) {
        props = new Properties();
        kafkaZk = conf.getString("kafka_test_zookeeper");
        id = consumerId;

        //Set by your own kafka server name
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
               "10.10.150.213:9097,10.10.150.214:9097,10.10.150.216:9097,10.10.150.217:9097,10.10.150.41:9097");

        //earliest, latest, none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Set by your own kafka consumer groupx
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_xzx_06");

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
                    System.out.printf("[ID:%d] offset = %d, key = %s, value = %s%n",
                            id, record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }



    public static void test1(){
        /*

         */
        ConsumerKafka02 csm0 = new ConsumerKafka02(1);
        csm0.SubscribeTopicPartition(Arrays.asList(new TopicPartition("caijing_risk_control",0)));
        csm0.DealMessage();


        ConsumerKafka02 csm1 = new ConsumerKafka02(1);
        final String topic = "caijing_risk_control";
        csm1.SubscribeTopicPartition(Arrays.asList(
                new TopicPartition(topic,1),
                new TopicPartition(topic,2),
                new TopicPartition(topic,3),
                new TopicPartition(topic,4)));

        csm1.DealMessage();
    }


    public static void test2(){
        ConsumerKafka02 csm0 = new ConsumerKafka02(1);
        System.out.println(csm0.consumer.subscription().size());
    }

    public static void main(String[] args){
        test1();
    }



}
