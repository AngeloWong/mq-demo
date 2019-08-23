package com.angelo.kafka.admin;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Properties;

public class KafkaAdmin {

    public static void createTopic() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        System.out.println(JaasUtils.isZkSecurityEnabled());
        AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(),
                AdminUtils.createTopic$default$6());
        zkUtils.close();
    }

    public static void deleteTopic() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        // 删除 topic 't1'
        AdminUtils.deleteTopic(zkUtils, "t1");
        zkUtils.close();
    }

    public static void listTopic() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        List<String> list = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
        for (String s : list) {
            System.out.println(s);
        }
        zkUtils.close();
    }
}
