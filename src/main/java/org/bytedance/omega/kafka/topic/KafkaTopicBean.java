package org.bytedance.omega.kafka.topic;

/**
 * Created by lkpnotice on 2/9/2018.
 */
public class KafkaTopicBean {
    private String topicName;
    private Integer partition;
    private Integer replication;
    private String desc;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Integer getReplication() {
        return replication;
    }

    public void setReplication(Integer replication) {
        this.replication = replication;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }


    @Override
    public String toString() {
        return "KafkaTopicBean [topicName=" + topicName + ", partition=" + partition
                + ", replication=" + replication + ", desc=" + desc +"]";
    }
}
