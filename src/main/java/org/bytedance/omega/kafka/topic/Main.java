package org.bytedance.omega.kafka.topic;

/**
 * Created by lkpnotice on 2/9/2018.
 */
public class Main {

    public static void testCreateTopic(){
        String ZkStr = "127.0.0.1:2183";

        KafkaTopicBean topic = new KafkaTopicBean();
        topic.setTopicName("testTopic");
        topic.setPartition(2);
        topic.setReplication(1);

        KafkaUtil.createKafkaTopic(ZkStr,topic);
       // KafkaUtil.deleteKafkaTopic(ZkStr,topic);
    }

    public static void testQueryTopic(){
        String ZkStr = "127.0.0.1:2183";
        String topic = "testTopic";
        KafkaUtil.queryKafkaTopic(ZkStr,topic);
    }

    public static void main(String args[]){
        testQueryTopic();
    }

}
