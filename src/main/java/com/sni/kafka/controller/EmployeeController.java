package com.sni.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sni.kafka.config.Sender;
import com.sni.kafka.model.Employee;

@RestController
public class EmployeeController {

	private static final Logger log = LoggerFactory.getLogger(EmployeeController.class);
	
	@Autowired
	private Sender sender;
	
	@RequestMapping("/hello")
	@GetMapping
	public String sayHello(){
		return "Hello Kafka";
	}
	
	@RequestMapping("/employee")
	@PutMapping(consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
	public Employee send(Employee emp){
		log.debug("Employee: {} ", emp);
		sender.send(emp);
		
		return emp;
	}
}
