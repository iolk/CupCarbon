package kakfa_connect;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConnect {
	private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

	public static void produce(final String topic, final String value) throws Exception {
		// Create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		try {
			// Create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

			// send data
			producer.send(record);
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static String consume(final String topic) throws Exception {
		// Creating Consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sap");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Creating consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		String result = "none";

		try {
			// Subscribing
			consumer.subscribe(Arrays.asList(topic));

			// Polling
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : records)
				result += "Key: "+ record.key() + ", Value:" +record.value()+" :: ";

			System.out.println(result);
		} finally {
			consumer.close();
		}

		return result;
	}
}
