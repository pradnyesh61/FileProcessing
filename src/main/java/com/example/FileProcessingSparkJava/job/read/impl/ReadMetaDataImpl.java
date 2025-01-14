package com.example.FileProcessingSparkJava.job.read.impl;

import com.example.FileProcessingSparkJava.job.model.ColumnMetadata;
import com.example.FileProcessingSparkJava.job.read.ReadMetaData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.example.FileProcessingSparkJava.constants.ApplicationConstants.COMMA;

@Service
public class ReadMetaDataImpl implements ReadMetaData {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadMetaDataImpl.class);

    private final SparkSession sparkSession;

    public ReadMetaDataImpl(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public List<StructField> read(String metaDataFilePath) throws URISyntaxException, IOException {
        LOGGER.info("reading MetaData ::");
        List<StructField> fields = new ArrayList<>();

        List<ColumnMetadata> columnMetadataList = new ArrayList<>();

        List<String> lines = test(metaDataFilePath);

        for (String line : lines) {
            String[] parts = line.split(COMMA);
            String columnName = parts[0];

            fields.add(DataTypes.createStructField(columnName, DataTypes.StringType, true));
            columnMetadataList.add(new ColumnMetadata(parts[0],parts[1], Integer.parseInt(parts[2]), Integer.parseInt(parts[3])));

        }

        return fields;
    }

    @Override
    public Dataset<Row> readFlatFile(String filePath, StructType schema) {
        LOGGER.info("reading flatFile ::");

        Dataset<Row> datasetRow = sparkSession.read()
                .option("header", "true")
                .schema(schema)
                .csv(getClass().getClassLoader().getResource(filePath).toString());

        datasetRow.show();

        datasetRow.printSchema();


        return datasetRow;
    }


    public List<String> test(String metaDataFilePath) {
        URL resource = getClass().getClassLoader().getResource(metaDataFilePath);
        LOGGER.info(String.valueOf(resource));

        if (resource == null) {
            LOGGER.error("File not found in resources!");
        }

        try {

            // Convert URL to String Path
            // String filePath = Paths.get(resource.getPath()).toString();
            String filePath = Paths.get(resource.toURI()).toString();

            // Read the text file as RDD
            JavaRDD<String> lines = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD();

            // Perform operations on the RDD
            List<String> collectedLines = lines.collect();

            // Print the content of the file
            System.out.println("File content:");
            collectedLines.forEach(System.out::println);

            return collectedLines;

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }
}
