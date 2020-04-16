package com.sni.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import com.sni.kafka.model.Employee;

public class Sender {

	private static final Logger log = LoggerFactory.getLogger(Sender.class);
	
	@Value("${sni.kafka.topic}")
	private String topic;
	
	@Autowired
	private KafkaTemplate<String, Employee> kafkaTemplate;
	
	public void send(Employee employee){
		log.info("sending employee = {}", employee);
		kafkaTemplate.send(topic, employee.getId().toString(), employee);
	}
}
