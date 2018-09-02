package org.bytedance.omega.kafka.topic;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lkpnotice on 2/9/2018.
 */
public class KafkaUtil {
    public static void createKafkaTopic(String zkStr,KafkaTopicBean topic){
        ZkUtils zkUtils = ZkUtils.apply(zkStr,30000,30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.createTopic(zkUtils, topic.getTopicName(), topic.getPartition(), topic.getReplication(), new Properties(),RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();

    }


    public static void deleteKafkaTopic(String ZkStr,KafkaTopicBean topic){
        ZkUtils zkUtils = ZkUtils.apply(ZkStr, 30000, 30000,JaasUtils.isZkSecurityEnabled());

        AdminUtils.deleteTopic(zkUtils, topic.getTopicName());
        zkUtils.close();
    }

    public static void queryKafkaTopic(String ZkStr,String topic){
        ZkUtils zkUtils = ZkUtils.apply(ZkStr, 30000, 30000,JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(),topic);
        Iterator iter = props.entrySet().iterator();

        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(String.format("[key:%s, value:%s]",key,value));
        }

        zkUtils.close();
    }


    public static void modifyTopic(String ZkStr,String topic){

    }

}
