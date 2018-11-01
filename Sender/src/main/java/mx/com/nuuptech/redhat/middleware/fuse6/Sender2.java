package mx.com.nuuptech.redhat.middleware.fuse6;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sender2 {
	
	final static Logger LOGGER = LoggerFactory.getLogger(Sender2.class);
	private MulticastSocket sender;
	private final static String MULTICAST_ADDRESS = "230.0.0.2";
	private final static Integer MULTICAST_PORT = 55558;
	
	public Sender2 () {
		try {
			this.sender = new MulticastSocket();
		} catch(IOException ioe) {
			LOGGER.error("Error creating sender" ,ioe);
		}
	}
	
	public void send(byte [] bytes) {
		DatagramPacket datagramPacket;
		try {
			datagramPacket = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(MULTICAST_ADDRESS), MULTICAST_PORT);
			this.sender.send(datagramPacket);
		} catch (UnknownHostException uhe) {
			LOGGER.error("Error creating Datagram Packet",uhe);
		} catch (IOException ioe) {
			LOGGER.error("Error sendig Datagram Packet",ioe);
		}
	}

}