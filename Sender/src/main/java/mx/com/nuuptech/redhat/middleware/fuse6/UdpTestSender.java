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
		Sender2 sender2 = new Sender2();
		Generator generator = new Generator();
		long marchOrder=199008170000000000L;
		for(long sequence = 1; sequence <= 120000; sequence = sequence + 6) {
			sender.send(generator.generate(sequence,marchOrder));
			sender2.send(generator.generate(sequence,marchOrder));
			marchOrder=marchOrder+2;
			try {
				TimeUnit.MILLISECONDS.sleep(1);
			} catch (InterruptedException e) {
				LOGGER.error("Error sleeping" , e);
			}
		}
		LOGGER.info("Stoping test at " + new SimpleDateFormat("H:m:s.S").format(new Date()));
	}

}
