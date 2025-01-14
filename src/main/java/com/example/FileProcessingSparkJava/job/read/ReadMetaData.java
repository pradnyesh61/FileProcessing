package com.example.FileProcessingSparkJava.job.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public interface ReadMetaData {

    List<StructField> read(String file) throws URISyntaxException, IOException;

    Dataset<Row> readFlatFile(String file, StructType schema);
}
