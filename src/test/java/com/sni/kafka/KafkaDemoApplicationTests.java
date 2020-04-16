package com.sni.kafka;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.Marshaller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.sni.kafka.external.deserializer.XMLDeserializer;
import com.sni.kafka.external.deserializer.XMLDeserializerConfig;
import com.sni.kafka.external.serializer.XMLSerializer;
import com.sni.kafka.model.Employee;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDemoApplicationTests {

	private static final Logger log = LoggerFactory.getLogger(KafkaDemoApplicationTests.class);

	XMLSerializer<Employee> serializer;
	XMLDeserializer<Employee> deserializer;

	@Before
	public void before() {
		this.serializer = new XMLSerializer<>();
		this.deserializer = new XMLDeserializer<>();

		Map<String, String> settings = new HashMap<>();
		settings.put(XMLDeserializerConfig.DESERIALIZED_CLASS_CONF, Employee.class.getName());
		settings.put(Marshaller.JAXB_FORMATTED_OUTPUT, "true");

		this.serializer.configure(settings, false);
		this.deserializer.configure(settings, false);
	}

	@Test
	public void testSerilization() {
		final Employee expected = new Employee(1L, "Test Employee", new Date());

		final byte[] buffer = this.serializer.serialize("testing", expected);
		log.info("Serialized as\n{}", new String(buffer, Charset.forName("UTF-8")));

		final Employee actual = this.deserializer.deserialize("testing", buffer);

		assertNotNull(actual);
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getJoiningDate(), actual.getJoiningDate());
	}

}
