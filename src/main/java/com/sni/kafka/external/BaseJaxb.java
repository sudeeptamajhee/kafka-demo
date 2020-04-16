package com.sni.kafka.external;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sni.kafka.external.serializer.XMLSerializerConfig;

public class BaseJaxb {

	private static final Logger log = LoggerFactory.getLogger(BaseJaxb.class);

	private ConcurrentMap<Class<?>, JAXBContext> contexts = new ConcurrentHashMap<>();

	protected JAXBContext context(Class<?> cls) {
		log.info("context() - cls = '{}'", cls);
		JAXBContext result = contexts.get(cls);

		if (null == result) {
			log.info("context() - No cached context for '{}', creating...", cls);
			try {
				result = JAXBContext.newInstance(cls);
			} catch (JAXBException e) {
				log.error("Exception thrown creating JAXBContext", e);
				throw new KafkaException("Exception thrown creating JAXBContext.", e);
			}
			this.contexts.putIfAbsent(cls, result);
		}

		return result;
	}

	protected Marshaller createMarshaller(Class<?> cls, XMLSerializerConfig config) {
		log.info("createMarshaller() - cls = '{}'", cls);
		JAXBContext context = context(cls);
		try {
			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_ENCODING, config.encoding);
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, config.formattedOutput);
			marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
			return marshaller;
		} catch (JAXBException e) {
			log.error("Exception thrown while creating Marshaller", e);
			throw new KafkaException("Exception thrown while creating Marshaller", e);
		}
	}

	protected Unmarshaller createUnmarshaller(Class<?> cls) {
		log.info("createUnmarshaller() - cls = '{}'", cls);
		JAXBContext context = context(cls);
		try {
			return context.createUnmarshaller();
		} catch (JAXBException e) {
			log.error("Exception thrown while creating Unmarshaller", e);
			throw new KafkaException("Exception thrown while creating Unmarshaller", e);
		}
	}
}
