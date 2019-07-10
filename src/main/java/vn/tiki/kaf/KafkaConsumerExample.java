package vn.tiki.kaf;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    public static boolean syncCommit = false;

    public static void main(String[] args)throws Exception {
        runConsumer();
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer("abc" + System.currentTimeMillis());

        final int giveUp = 100;
        int noRecordsCount = 0;

        long timeBegin = System.nanoTime();

        int numberMessageToConsume = 10000;

        int count = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            count += consumerRecords.count();

            if (count > numberMessageToConsume) {
                break;
            }

//            consumerRecords.forEach(record -> {
//                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                        record.key(), record.value(),
//                        record.partition(), record.offset());
//            });

            //consumer.commitAsync();
            consumer.commitSync();
        }
        consumer.close();

        long timeEnd = System.nanoTime();

        float rate = ((float) count) * 1000000000 / (timeEnd - timeBegin);

        String format = String.format("%.02f", rate);

        if (syncCommit) {
            System.out.println("Current   (sync) Procedure rate: " + format);
        } else {
            System.out.println("Current (!async) Procedure rate: " + format);
        }
    }

    public static Consumer<Long, String> createConsumer(String groupName) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));

        return consumer;

    }

}
