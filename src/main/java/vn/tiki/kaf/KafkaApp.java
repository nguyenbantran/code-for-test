package vn.tiki.kaf;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaApp {

    public static void main(String[] args) throws Exception {
        runProducer();
    }

    static boolean syncCommit = true;

    static void runProducer() {

        Thread.currentThread().setContextClassLoader(null);

        Producer<Long, String> producer = ProducerCreator.createProducer();

        int numberMessageToProcedure = 100*1000;

        if (syncCommit) {
            numberMessageToProcedure = 1000;
        }



        long timeBegin = System.nanoTime();

        for (int index = 0; index < numberMessageToProcedure; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, "This is record " + index);
            try {
                if (syncCommit) {
                    RecordMetadata metadata = producer.send(record).get();
                    //System.out.println("Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset());
                } else {
                    producer.send(record);
                }


            } catch (Exception e) {
                System.out.println("Error in sending record");
                System.out.println(e);

            }
//            catch (InterruptedException e) {
//                System.out.println("Error in sending record");
//                System.out.println(e);
//            }
        }

        producer.close();

        long timeEnd = System.nanoTime();

        float rate = ((float) numberMessageToProcedure) * 1000000000 / (timeEnd - timeBegin);

        String format = String.format("%.02f", rate);

        if (syncCommit) {
            System.out.println("Current   (sync) Procedure rate: " + format);
        } else {
            System.out.println("Current (!async) Procedure rate: " + format);
        }



    }
}
