package com.example.FileProcessingSparkJava.job;

import java.io.IOException;
import java.net.URISyntaxException;

public abstract class Job {

    abstract public void preProcess() throws URISyntaxException, IOException;

    public void execute() throws URISyntaxException, IOException {
        preProcess();
    }
}
