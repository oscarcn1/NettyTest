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

	public byte[] generate(Long sequence, Long ordenMatch) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write(this.generateSession()); // Sesion 10 bytes
			baos.write(this.convertLongToByteArray(sequence)); // Secuencia 8 bytes
			baos.write(this.convertShortToByteArray((short) 6)); // Numero de mensajes en el paquete 2 bytes
			
			baos.write(this.convertShortToByteArray((short) 5)); // Longitud del primer mensaje 2 bytes
			baos.write(this.generateMessageT()); // Mensaje T 5 bytes
			
			
			baos.write(this.convertShortToByteArray((short) 30));
			baos.write(this.generateMessageA(ordenMatch));
			
			baos.write(this.convertShortToByteArray((short) 31));
			baos.write(this.generateMessageE(ordenMatch));
			
			baos.write(this.convertShortToByteArray((short) 14));
			baos.write(this.generateMessageB(ordenMatch));
			
			baos.write(this.convertShortToByteArray((short) 33));
			baos.write(this.generateMessageU(ordenMatch));
			
			
			baos.write(this.convertShortToByteArray((short) 13));
			baos.write(this.generateMessageD(ordenMatch + 1L));

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

	private byte[] generateMessageB(Long matchNumber) {
		byte bytes[] = new byte[14];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'B');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(matchNumber);
		byteBuffer.put((byte) 'H');
		return bytes;
	}

	private byte[] generateMessageA(Long numeroOrden) {
		byte bytes[] = new byte[30];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'A');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(numeroOrden);
		byteBuffer.put((byte) 'B');
		byteBuffer.putLong(5000L);
		byteBuffer.putInt(80000);
		byteBuffer.putInt(80000);
		return bytes;
	}

	private byte[] generateMessageU(Long numeroOrden) {
		byte bytes[] = new byte[33];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'U');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(numeroOrden);
		byteBuffer.putLong(numeroOrden + 1L);
		byteBuffer.putLong(5000L);
		byteBuffer.putInt(80000);
		return bytes;
	}

	private byte[] generateMessageE(Long matchNumber) {
		byte bytes[] = new byte[30];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'E');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(matchNumber);
		byteBuffer.putLong(1L);
		byteBuffer.putLong(matchNumber);
		byteBuffer.put((byte) 'R');
		return bytes;
	}

	private byte[] generateMessageD(Long orderNumber) {
		byte bytes[] = new byte[13];
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.put((byte) 'D');
		byteBuffer.putInt(500000);
		byteBuffer.putLong(orderNumber);
		return bytes;
	}

}