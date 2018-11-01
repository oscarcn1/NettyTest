package mx.com.nuuptech.redhat.middleware.fuse6;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generator {

	final static Logger LOGGER = LoggerFactory.getLogger(Generator.class);

	public byte[] generate(Long sequence) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write(this.generateSession());							//Sesion 10 bytes
			baos.write(this.convertLongToByteArray(sequence));			//Secuencia 8 bytes
			baos.write(this.convertShortToByteArray((short) 4));		//Numero de mensajes en el paquete 2 bytes
			baos.write(this.convertShortToByteArray((short) 5));		//Longitud del primer mensaje 2 bytes
			baos.write(this.generateMessageT());						//Mensaje T 5 bytes
			baos.write(this.convertShortToByteArray((short) 14));
			baos.write(this.generateMessageB());
			baos.write(this.convertShortToByteArray((short) 14));
			baos.write(this.generateMessageB());
			baos.write(this.convertShortToByteArray((short) 14));
			baos.write(this.generateMessageB());
		} catch (IOException ioe) {
			LOGGER.error("Error creating byte array", ioe);
		}
		return baos.toByteArray();
	}

	private byte[] generateSession() {
		byte bytes[] = new byte[10];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 's');
		byteBuffer.put((byte) 'e');
		byteBuffer.put((byte) 's');
		byteBuffer.put((byte) 's');
		byteBuffer.put((byte) 'i');
		byteBuffer.put((byte) 'o');
		byteBuffer.put((byte) 'n');
		byteBuffer.put((byte) '0');
		byteBuffer.put((byte) '0');
		byteBuffer.put((byte) '1');
		return bytes;
	}

	private byte[] convertLongToByteArray(long l) {
		byte bytes[] = new byte[8];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.putLong(l);
		return bytes;
	}
	
	private byte[] convertShortToByteArray(short s) {
		byte bytes[] = new byte[2];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.putShort(s);
		return bytes;
	}
	
	private byte[] generateMessageT() {
		byte bytes[] = new byte[5];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'T');
		LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC-6"));
		Integer secondOfDay = now.get(ChronoField.SECOND_OF_DAY);
		byteBuffer.putInt(secondOfDay.intValue());
		return bytes;
	}
	
	private byte[] generateMessageB() {
		byte bytes[] = new byte[14];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'B');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(1000L);
		byteBuffer.put((byte) 'H');
		return bytes;
	}

}