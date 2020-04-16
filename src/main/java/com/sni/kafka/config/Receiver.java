package com.sni.kafka.config;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import com.sni.kafka.model.Employee;

public class Receiver {

	private static final Logger log = LoggerFactory.getLogger(Receiver.class);
	
	private CountDownLatch latch = new CountDownLatch(1);
	
	public CountDownLatch getLatch(){
		return latch;
	}
	
	@KafkaListener(topics="${sni.kafka.topic}")
	public void receive(Employee employee){
		log.info("received employee = {}", employee);
		latch.countDown();
	}
}
