package mx.com.actinver.biva.decoder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.databind.ObjectMapper;
import mx.com.biva.itch.modelo.*;
import io.netty.buffer.ByteBuf;


public class ItchDecoder implements Processor {
	
	@Produce(uri = "activemq:queue:BIVA.ITCH.FEED.SECUENCIAS.RETRANSMISION")
	private ProducerTemplate retransmision;
	
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS");
	private LocalDateTime todayMidnight;
	private Long lastTimeStamp;

	private final static String YAML_CONFIGURATION_FILE = "itchBiva.yaml";
	final static Logger LOGGER = LoggerFactory.getLogger(ItchDecoder.class);
	
	private Map<Object, Object> yMap;
	private Map<Object, Object> fMap;
	private Map<Object, Object> mMap;
	private Yaml yaml;
	private Boolean isFirstMessage = true;
	private ObjectMapper objectMapper = new ObjectMapper();
	
	private Long secuencia = new Long(0L);
	private String sesion = new String();
	
	private StringWriter stringWriter = null;
	
	public void process(Exchange exchange) throws Exception {
		byte[] bytes;
		ByteBuf buffer = (ByteBuf) exchange.getIn().getBody();
		int length = buffer.readableBytes();
		if (buffer.hasArray()) {
			bytes = buffer.array();
		} else {
			bytes = new byte[length];
			buffer.getBytes(buffer.readerIndex(), bytes);
		}
		List<Message> mensajes = this.decodeMessages(bytes);
		if(this.isFirstMessage.booleanValue()) {
			this.isFirstMessage = new Boolean(false);
			retransmision.sendBody("{\"secuencia\":" + String.valueOf(secuencia) + ", \"session\":\"" + sesion + "\"}");
		}
		List<String> mensajesJSON = new ArrayList<String>();
		for(Message mensaje : mensajes) {
			stringWriter = new StringWriter();
			objectMapper.writeValue(stringWriter, mensaje);
			mensajesJSON.add(stringWriter.toString());
		}
		exchange.getIn().setBody(mensajesJSON);	
	}
	
	@SuppressWarnings("unchecked")
	public void inicializar() throws FileNotFoundException {
		yaml = new Yaml();
		Object y = yaml.load(this.getClass().getClassLoader().getResourceAsStream(YAML_CONFIGURATION_FILE));
		yMap = (Map<Object, Object>) y;
		buildFormats();
		buildMessages();
		LocalTime midnight = LocalTime.MIDNIGHT;
		LocalDate today = LocalDate.now(ZoneId.of("UTC-6"));
		this.todayMidnight = LocalDateTime.of(today, midnight);
	}
	
	public void destruir() {
		LOGGER.info("Destruyendo el lector.");
	}
	
	/**
	 * 
	 * @param payload
	 * @return
	 * @throws IOException
	 */
	private List<Message> decodeMessages(byte[] payload) throws IOException {
		List<Message> mensajes = new ArrayList<Message>();
		ArrayList<String> messageArray = null;
		sesion = getString(Arrays.copyOfRange(payload, 0, 10), 10);
		secuencia = getLong(Arrays.copyOfRange(payload, 10, 18));
		long numeroDeMensajes = getInt(Arrays.copyOfRange(payload, 18, 20), 2);
		long longitudDelPrimerMensaje = getInt(Arrays.copyOfRange(payload, 20, 22), 2);
		Message message = null;
		int messagePointer = 0;
		int offset = 22;
		for (long contador = 0; contador < numeroDeMensajes; contador = contador + 1L) {
			messageArray = new ArrayList<String>();
			payload = Arrays.copyOfRange(payload, offset, payload.length);
			String tipoDeMensaje = this.getString(Arrays.copyOfRange(payload, 0, 1), 1);
			messageArray.add(tipoDeMensaje);
			messagePointer = 1;
			ArrayList<Object> fieldsArray = null;
			try {
				fieldsArray = this.getFields(tipoDeMensaje);
			} catch (Exception exception) {
				secuencia = secuencia + 1L;
				LOGGER.error("Error al obtener la configuración del mensaje con tipo " + tipoDeMensaje, exception);
			}
			if (fieldsArray != null) {
				int messagePointerFinal = 0;
				for (int i = 1; i < fieldsArray.size(); i++) {
					ArrayList<Object> fieldArray = this.getFormat((String) fieldsArray.get(i));
					messagePointerFinal = (messagePointer + ((Integer) fieldArray.get(1)));
					messageArray.add(parse(Arrays.copyOfRange(payload, messagePointer, messagePointerFinal), messagePointerFinal - messagePointer, fieldArray));
					messagePointer = messagePointer + ((Integer) fieldArray.get(1));
				}
				offset = messagePointerFinal + 2;
				message = this.buildBean(messageArray);
				message.setSecuencia(secuencia);
				secuencia = secuencia + 1L;
				if (message.getType().charValue() == 'T') {
					this.setLastTimeStamp(((T) message).getTimestamp().longValue());
				}
				message.setFechaHoraRecepcion(this.simpleDateFormat.format(new Date()));
				message.setSession(sesion);
				message.setFechaHoraEmision(this.getTimeFromMidnightPlusSecondsAndNanoSeconds(this.getLastTimeStamp(), message.getTimestamp().longValue()));
				mensajes.add(message);
			} else {
				LOGGER.error("No se encontraron campos definidos en el archivo YAML para el tipo de mensaje: " + tipoDeMensaje);
			}
		}
		return mensajes;
	}

	@SuppressWarnings("unchecked")
	private void buildFormats() {
		fMap = new HashMap<>();
		ArrayList<Object> fArray = (ArrayList<Object>) yMap.get("formats");
		for (int i = 0; i < fArray.size(); ++i) {
			Map<Object, Object> tempMap = (Map<Object, Object>) (fArray.get(i));
			fMap.put(tempMap.keySet().toArray()[0], tempMap.values().toArray()[0]);
		}
	}

	@SuppressWarnings("unchecked")
	private void buildMessages() {
		mMap = (Map<Object, Object>) yMap.get("messages");
	}

	@SuppressWarnings("unchecked")
	private ArrayList<Object> getFields(String mType) {
		Map<Object, Object> tempMap = (Map<Object, Object>) mMap.get(mType);
		assert tempMap != null : "File type missing: " + mType;
		return (ArrayList<Object>) tempMap.get("fields");
	}

	@SuppressWarnings("unchecked")
	private ArrayList<Object> getFormat(String field) {
		Object fieldVal = fMap.get(field);
		assert fieldVal != null : "Format field missing: " + field;
		return (ArrayList<Object>) fieldVal;
	}

	@SuppressWarnings("unchecked")
	private String getFieldName(String mType) {	
		Map<Object, Object> tempMap = (Map<Object, Object>) mMap.get(mType);
		return tempMap.get("name").toString();
	}
	
	/**
	 * 
	 * @param payload
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private String getString(byte[] payload, int length) throws UnsupportedEncodingException {
		byte[] bytes = new byte[length];
		ByteBuffer buffer = ByteBuffer.wrap(payload);
		buffer.get(bytes);
		return new String(bytes, StandardCharsets.US_ASCII);
	}

	/**
	 * 
	 * @param payload
	 * @return
	 */
	private Object getLen(byte[] payload) {
		ByteBuffer length = ByteBuffer.wrap(payload);
		return (int) length.getShort(0);
	}

	/**
	 * 
	 * @param payload
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private long getInt(byte[] payload, int length) throws UnsupportedEncodingException {
		long value = 0L;
		try {
			for (int i = 0; i < payload.length; i++) {
				value = (value << 8) + (payload[i] & 0xFF);
			}

		} catch (Exception eee) {
			LOGGER.error("Malformed numeric: " + payload);
		}

		return value;
	}

	/**
	 * 
	 * @param payload
	 * @return
	 * @throws NumberFormatException
	 * @throws UnsupportedEncodingException
	 */
	private String getDouble(byte[] payload, int length) throws NumberFormatException, UnsupportedEncodingException {
		return ("" + (Double.parseDouble("" + getInt(payload, length)) / 10000));
	}

	/**
	 * 
	 * @param arr
	 * @param fieldArray
	 * @return
	 * @throws IOException
	 */
	private String parse(byte[] arr, int length, ArrayList<Object> fieldArray) throws IOException {
		String value = "";
		switch ((Integer) fieldArray.get(2)) {
		case 1:
			value = getString(arr, length);
			break;
		case 2:
			value = "" + Long.parseLong("" + getInt(arr, length));
			break;
		}
		return value.trim();
	}

	/**
	 * 
	 * @param messageArray
	 * @return
	 */
	private Message buildBean(ArrayList<String> messageArray) {
		Message message = null;
		switch (messageArray.get(0)) {
		case "A":
			message = new A(messageArray);
			break;
		case "B":
			message = new B(messageArray);
			break;
		case "C":
			message = new C(messageArray);
			break;
		case "D":
			message = new D(messageArray);
			break;
		case "E":
			message = new E(messageArray);
			break;
		case "F":
			message = new F(messageArray);
			break;
		case "G":
			message = new G(messageArray);
			break;
		case "H":
			message = new H(messageArray);
			break;
		case "I":
			message = new I(messageArray);
			break;
		case "L":
			message = new L(messageArray);
			break;
		case "a":
			message = new LowerCaseA(messageArray);
			break;
		case "e":
			message = new LowerCaseE(messageArray);
			break;
		case "M":
			message = new M(messageArray);
			break;
		case "N":
			message = new N(messageArray);
			break;
		case "P":
			message = new P(messageArray);
			break;
		case "Q":
			message = new Q(messageArray);
			break;
		case "R":
			message = new R(messageArray);
			break;
		case "S":
			message = new S(messageArray);
			break;
		case "T":
			message = new T(messageArray);
			break;
		case "U":
			message = new U(messageArray);
			break;
		case "X":
			message = new X(messageArray);
			break;
		default:
			LOGGER.error("Not Found MessageType Builder:" + messageArray);
		}
		return message;
	}

	private Long getLong(byte[] bytes) throws UnsupportedEncodingException {
		long value = 0L;
		try {
			for (int i = 0; i < bytes.length; i++) {
				value = (value << 8) + (bytes[i] & 0xFF);
			}
		} catch (Exception exception) {
			LOGGER.error("No se puede obtener un long de: " + bytes, exception);
		}
		return value;
	}
	
	private String getTimeFomMidnightPlusSeconds(Long seconds) {
		LocalDateTime newTime = this.todayMidnight.plusSeconds(seconds);
		return newTime.format(formatter);
	}
	
	private String getTimeFromMidnightPlusSecondsAndNanoSeconds(Long seconds, Long nanoSeconds) {
		seconds = seconds != null ? seconds : 0;
		nanoSeconds = nanoSeconds != null ? nanoSeconds : 0;
		LocalDateTime newTime = this.todayMidnight.plusSeconds(seconds).plusNanos(nanoSeconds);
		return newTime.format(formatter);
	}

	private Long getLastTimeStamp() {
		return lastTimeStamp;
	}

	private void setLastTimeStamp(Long lastTimeStamp) {
		this.lastTimeStamp = lastTimeStamp;
	}

	private Boolean getIsFirstMessage() {
		return isFirstMessage;
	}

	private void setIsFirstMessage(Boolean isFirstMessage) {
		this.isFirstMessage = isFirstMessage;
	}

}