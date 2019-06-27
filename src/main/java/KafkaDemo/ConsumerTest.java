package KafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * describe: Kerberos环境下通过Kafka的Bootstrap.Server消费数据
 * creat_date: 2019/06/11
 */
public class ConsumerTest {

    public static String confPath = System.getProperty("user.dir")  + File.separator + "conf";

    public static void main(String[] args) {
        String krb5conf = confPath + File.separator + "krb5.conf";
        String jaasconf = confPath + File.separator + "jaas.conf";
        Properties appProperties = new Properties();
        try {
            appProperties.load(new FileInputStream(new File(confPath + File.separator + "app.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String brokerlist = String.valueOf(appProperties.get("bootstrap.servers"));
        String topic_name = String.valueOf(appProperties.get("topic.name"));

        System.setProperty("java.security.krb5.conf", krb5conf);
        System.setProperty("java.security.auth.login.config", jaasconf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true");

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer2");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.put("security.protocol", "SASL_PLAINTEXT");
        consumerConfig.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
        TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
        consumer.subscribe(Collections.singletonList(topic_name), rebalanceListener);

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            consumer.commitSync();
        }
    }

    private static class  TestConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }

}
