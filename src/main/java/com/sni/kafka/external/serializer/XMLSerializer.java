package com.sni.kafka.external.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sni.kafka.external.BaseJaxb;

public class XMLSerializer<T> extends BaseJaxb implements Serializer<T> {

	private static final Logger log = LoggerFactory.getLogger(XMLSerializer.class);
	
	XMLSerializerConfig config;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.config = new XMLSerializerConfig(configs);
	}

	@Override
	public byte[] serialize(String topic, T data) {
		if (null == topic) {
			return null;
		}

		Marshaller marshaller = createMarshaller(data.getClass(), this.config);
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			marshaller.marshal(data, outputStream);
			return outputStream.toByteArray();
		} catch (IOException | JAXBException ex) {
			log.error("Exception thrown while serializing value", ex);
			throw new KafkaException("Exception thrown while serializing value", ex);
		}
	}

	@Override
	public void close() {

	}
}
