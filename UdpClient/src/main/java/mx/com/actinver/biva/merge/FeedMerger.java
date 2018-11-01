package mx.com.actinver.biva.merge;

import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeedMerger {
	
	final static Logger LOGGER = LoggerFactory.getLogger(FeedMerger.class);
	@Produce(uri = "activemq:topic:BIVA.ITCH.FEED?deliveryPersistent=true")
	private ProducerTemplate feedUnico;
	private Long lastSequenceNumber = new Long(-1L);
	private static final String SECUENCIA = "\"secuencia\":";
	private static final String ERROR = "No se logro agregar el mensaje al feed unico ";
	private static final String COMMA = ",";
	private static final String LINE_BREAK = System.getProperty("line.separator");
	private static final String VOID = "";
	
	public synchronized String addMessage(String message) {
		String respuesta = new String(VOID);
		try {
			Long sequence = this.lastSequenceNumber;
			Integer indiceInicio = message.indexOf(SECUENCIA) + 13;
			Integer indiceFin = indiceInicio.intValue() + message.substring(indiceInicio.intValue()).indexOf(COMMA);
			sequence = Long.valueOf(message.substring(indiceInicio.intValue(), indiceFin.intValue()));
			if(this.lastSequenceNumber.longValue() < sequence.longValue()) {
				this.lastSequenceNumber = sequence;
				feedUnico.sendBody(message);
				respuesta = message + String.format(LINE_BREAK); 
			}
		} catch (Exception e) {
			LOGGER.error(ERROR + message,e);
		}
		return respuesta;
	}

}