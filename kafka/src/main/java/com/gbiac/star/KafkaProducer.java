package com.gbiac.star;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

public class KafkaProducer {

    private static Logger logger = Logger.getLogger(KafkaProducer.class);
   private final org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;

    public KafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers",KafkaProperties.KAFKA_BROKER_LIST);
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id","producerDemo");
        this.producer=new org.apache.kafka.clients.producer.KafkaProducer<Integer, String>(props);

    }
    public void sendMessage(){
        producer.send(new ProducerRecord<Integer, String>(KafkaProperties.TOPIC,1,"message"), new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println("message send to :[" + recordMetadata.partition() + "],offset:[" + recordMetadata.offset()+ "]");
                logger.info("logggggg:"+ recordMetadata.partition());

            }
        });
    }

    public static void main(String[] args) throws IOException {
        KafkaProducer producer=new KafkaProducer();
        producer.sendMessage();
        System.in.read();
    }
}
