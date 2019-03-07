package Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Arrays;
import java.util.Properties;

/**
 * kafka 消费者demo
 */
public class KafkaConsumerTest extends Thread {
    private String topic;

    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerTest(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("group.id", "wangjx");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        consumer = new KafkaConsumer<String, String>(properties);
    }

    @Override
    public void run() {
        // 订阅指定主题的全部分区
        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
        try {
            while (true) {
                /*
                 * poll() 方法返回一个记录列表。
                 * 每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对。
                 * 我们一般会遍历这个列表，逐条处理这些记录。
                 * 传给poll() 方法的参数是一个超时时间，用于控制 poll() 方法的阻塞时间（在消费者的缓冲区里没有可用数据时会发生阻塞）。
                 * 如果该参数被设为 0，poll() 会立即返回，否则它会在指定的毫秒数内一直等待 broker 返回数据。
                 * 而在经过了指定的时间后，即使还是没有获取到数据，poll()也会返回结果。
                 */
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /*
             * 在退出应用程序之前使用 close() 方法关闭消费者。
             * 网络连接和 socket 也会随之关闭，并立即触发一次再均衡，而不是等待群组协调器发现它不再发送心跳并认定它已死亡，
             * 因为那样需要更长的时间，导致整个群组在一段时间内无法读取消息。
             */
            consumer.close();
        }

    }
}
