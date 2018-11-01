package mx.com.nuuptech.redhat.middleware.fuse6;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdpTestSender {
	
	final static Logger LOGGER = LoggerFactory.getLogger(Generator.class);

	public static void main(String[] args) {
		LOGGER.info("Starting test at " + new SimpleDateFormat("H:m:s.S").format(new Date()));
		Sender sender = new Sender();
		Generator generator = new Generator();
		for(long sequence = 1; sequence <= 34560000; sequence = sequence + 4) {
			sender.send(generator.generate(sequence));
			try {
				TimeUnit.MILLISECONDS.sleep(3);
			} catch (InterruptedException e) {
				LOGGER.error("Error sleeping" , e);
			}
		}
		LOGGER.info("Stoping test at " + new SimpleDateFormat("H:m:s.S").format(new Date()));
	}

}
