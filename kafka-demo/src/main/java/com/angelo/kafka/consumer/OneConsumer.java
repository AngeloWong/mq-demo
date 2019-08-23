package com.angelo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class OneConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // key 反序列化器
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        // value 反序列化器
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("test-topic");
        List<TopicPartition> list = new ArrayList<>();
        list.add(new TopicPartition("test-topic", 0));

//        for (PartitionInfo partitionInfo : partitionInfos) {
//            list.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
//        }

        // topicA 01        topicB 02
        kafkaConsumer.assign(list);

        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
            for (ConsumerRecord<String, String> context : poll) {
                System.out.println("消息所在分区：" + context.partition() + " - 消息的偏移量：" + context.offset()
                        + " key: " + context.key() + " value: " + context.value());

                
            }
                    
        }
    }
}
