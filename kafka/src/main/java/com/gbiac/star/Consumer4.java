package com.gbiac.star;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer4 extends ShutdownableThread {

    private  final KafkaConsumer<Integer,String> consumer;

    public Consumer4() {
        super("KafkaConsumerTest", false);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_BROKER_LIST);
        //GroupId消息所属的分组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "DemonGroup2");
        //是否自动提交消息：offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交的间隔时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //设置使用最开始ffset偏移量为当前group.id的最早消息
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //设置心跳时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        //对key和value设置反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<Integer, String>(properties);

        //指定分区，与  consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));互斥
       /* TopicPartition p0=new TopicPartition(KafkaProperties.TOPIC,0);
        this.consumer.assign(Arrays.asList(p0));*/
    }
    //High level consumer
    //low level consumer

    public void doWork() {
        consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for(ConsumerRecord record:records){
            System.out.println("["+record.partition()+"]receive message:"+
                    "["+record.key()+"->"+record.value()+"],offset:"+record.offset()+"");

        }


    }

    public static void main(String[] args) {

        new Consumer4().start();

        //获取当前offset的当前位置
      //  System.out.println(Math.abs("DemonGroup1".hashCode()%50));
    }

}
