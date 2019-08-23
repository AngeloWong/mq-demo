package com.angelo.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class Consumer1 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // key 反序列化器
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        // value 反序列化器
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("enable.auto.commit", "false");
//        properties.setProperty("auto.offset.reset", "none");
        // 定义消费者群组
        properties.setProperty("group.id", "1111");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("test-topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("分区再均衡之前");
                kafkaConsumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("分区再均衡之后");
                for (TopicPartition partition : partitions) {
                    System.out.println("新分配到的分区：" + partition.partition());
                }
            }
        });

        // 关闭  优雅退出
        kafkaConsumer.close();

        // 同步提交偏移量
        kafkaConsumer.commitSync();
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
        offsetAndMetadataMap.put(new TopicPartition("test-topic", 0), new OffsetAndMetadata(8));

        try {
            while (true) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> context : poll) {
                    System.out.println("消息所在分区：" + context.partition() + " - 消息的偏移量：" + context.offset()
                            + " key: " + context.key() + " value: " + context.value());


                    System.out.println("处理消费到的消息逻辑");
                }
                // 异步提交偏移量
                kafkaConsumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 同步提交偏移量
                kafkaConsumer.commitSync();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}
