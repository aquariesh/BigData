package Kafka;

/**
 * kafka 测试类
 */
public class KafkaTest {
    public static void main(String[] args) {
        new KafkaProducerTest(KafkaProperties.TOPIC).start();

        new KafkaConsumerTest(KafkaProperties.TOPIC).start();
    }
}
