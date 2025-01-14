package com.example.FileProcessingSparkJava;

import com.example.FileProcessingSparkJava.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class FileProcessingSparkJavaApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessingSparkJavaApplication.class);

    private final Job job;

    public FileProcessingSparkJavaApplication(Job job) {
        this.job = job;
    }

    public static void main(String[] args) {
        LOGGER.info("Application Starting...");
        SpringApplication.run(FileProcessingSparkJavaApplication.class, args);
        LOGGER.info("Application ended.");
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {
            LOGGER.info("Running Job...");
            job.execute();
        };
    }
}
