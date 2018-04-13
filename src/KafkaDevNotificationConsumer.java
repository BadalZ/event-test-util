
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaDevNotificationConsumer {
	private final String topic;
	private final Properties props;

	public KafkaDevNotificationConsumer(String brokers, String topic, String username, String password) {
		this.topic = topic;

		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, username, password);

		String serializer = StringSerializer.class.getName();
		String deserializer = StringDeserializer.class.getName();
		props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", "newer");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", deserializer);
		props.put("value.deserializer", deserializer);
		props.put("key.serializer", serializer);
		props.put("value.serializer", serializer);
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "SCRAM-SHA-256");
		props.put("sasl.jaas.config", jaasCfg);
	}

	public void consume() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
		}
	}

	public void produce(final String message) {
		Thread one = new Thread() {
			public void run() {
				try {
					Producer<String, String> producer = new KafkaProducer<>(props);
					int i = 0;
					while (true) {
						Date d = new Date();
						String s = "{\"date:\"" + d.toString() + ","
								+ "\"projectId\": 11,\"phaseId\": 1032,\"phaseTypeName\": \"Registration\",\"state\": \"END\",\"operator\": \"operator 1\"}";
						producer.send(new ProducerRecord<>(topic, Integer.toString(i), s));
						Thread.sleep(5000);
						i++;
					}
					// producer.close();
				} catch (InterruptedException v) {
					System.out.println(v);
				}
			}
		};
		one.start();
	}

	public static void main(String[] args) {
		// String topic = System.getenv("CLOUDKARAFKA_TOPIC_PREFIX") + ".test";
		String topic = "notifications.kafka.queue.java.test";
		String brokers = "silver-craft-01.srvs.cloudkafka.com:9094";
		String username = "014x7w2b";
		String password = "sPnkjvwRg_nayz7gitsiZcWu1aJrlaiP";

		KafkaDevNotificationConsumer c = new KafkaDevNotificationConsumer(brokers, topic, username, password);

		//String data = "{\"type\": \"kafka.queue.java.test\",\"message\": \"{ \"data\": \"Sample Data\" }\"}";
		//c.produce(data);
		c.consume();
	}
}
