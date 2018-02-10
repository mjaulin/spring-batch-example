package fr.sii.atlantique.bdf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@EnableBatchProcessing
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        SpringApplication.run(Application.class, args);
        time = System.currentTimeMillis() - time;
        LOGGER.info("Runtime: {} seconds.", ((double)time/1000));
    }
}
