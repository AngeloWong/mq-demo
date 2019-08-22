package com.angelo.kafka.product;

import com.angelo.kafka.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // key 序列化器
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        // value 序列化器
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // 自定义分区管理器
        properties.setProperty("partitioner.class", MyPartitioner.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-topic", /*"test.key"*/ null,
                "hello");
        Future<RecordMetadata> send = kafkaProducer.send(record);
        RecordMetadata recordMetadata = send.get();
        System.out.println(recordMetadata.offset() + "-- topic: " + recordMetadata.topic());
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
