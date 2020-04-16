package com.sni.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.sni.kafka.external.deserializer.XMLDeserializer;
import com.sni.kafka.external.deserializer.XMLDeserializerConfig;
import com.sni.kafka.model.Employee;

@Configuration
@EnableKafka
public class ReceiverConfig {

	@Value("${sni.kafka.bootstrap-servers}")
	private String bootstrapServers;
	@Value("${sni.kafka.group-id}")
	private String groupId;
	@Value("${sni.kafka.client-id}")
	private String clientId;
	@Value("${sni.kafka.auto-offset-reset}")
	private String autoOffset;

	@Bean
	public Map<String, Object> consumerConfigs() {

		Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, XMLDeserializer.class);
		props.put(XMLDeserializerConfig.DESERIALIZED_CLASS_CONF, Employee.class);

		return props;
	}

	@Bean
	public ConsumerFactory<String, Employee> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Employee> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Employee> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}

	@Bean
	public Receiver receiver() {
		return new Receiver();
	}
}
