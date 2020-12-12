package KafkaAvroProducerV1;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV1 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        String topic = "customer-avro";
        Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAutomatedEmail(false)
                .build();
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(properties);
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null){
                System.out.println("Success!");
                System.out.println(recordMetadata.toString());
            }
            e.printStackTrace();
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
