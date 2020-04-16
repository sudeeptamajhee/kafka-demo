package com.sni.kafka.external.deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sni.kafka.external.BaseJaxb;

public class XMLDeserializer<T> extends BaseJaxb implements Deserializer<T> {

	private static final Logger log = LoggerFactory.getLogger(XMLDeserializer.class);
	
	XMLDeserializerConfig config;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.config = new XMLDeserializerConfig(configs);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(String topic, byte[] data) {
		if (null == data) {
			return null;
		}

		try {
			ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
			Unmarshaller unmarshaller = createUnmarshaller(this.config.deserializedClass);
			return (T) unmarshaller.unmarshal(new StreamSource(inputStream));
		} catch (JAXBException ex) {
			log.error("Exception thrown while deserializing value", ex);
			throw new KafkaException("Exception thrown while deserializing value", ex);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}
