package com.example.FileProcessingSparkJava.service;

import com.example.FileProcessingSparkJava.job.Job;
import com.example.FileProcessingSparkJava.job.read.ReadMetaData;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

@Component
public class ReadFile extends Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFile.class);

    private final ReadMetaData readMetaData;

    public ReadFile(ReadMetaData readMetaData) {
        this.readMetaData = readMetaData;
    }

    @Override
    public void preProcess() throws URISyntaxException, IOException {
        LOGGER.info("Reading metadata...");
        List<StructField> fields = readMetaData.read("metaData.txt");
        StructType schema = DataTypes.createStructType(fields);

        LOGGER.info("Reading flat file...");
        Dataset<Row> flatFileDataset =  readMetaData.readFlatFile("flatFile.txt", schema);
        LOGGER.info("Got------------------------------------------ flat file...");
        LOGGER.info("Validating flat file...");
        Dataset<Row> validatedDataset = validateData(flatFileDataset, schema);

        LOGGER.info("Storing validated data into H2 database...");
       // storeToH2Database(validatedDataset, "validated_table");

        LOGGER.info("Process ...");
    }



    private Dataset<Row> validateData(Dataset<Row> flatFileDataset, StructType schema) {
        return flatFileDataset.filter((FilterFunction<Row>) row -> {
            for (StructField field : schema.fields()) {
                if (row.isNullAt(row.fieldIndex(field.name()))) {
                    return false; // Reject rows with null values
                }
            }
            return true;
        });
    }

//    private void storeToH2Database(Dataset<Row> validatedDataset, String validatedTable) {
//        validatedDataset.write()
//                .format("jdbc")
//                .option("url", "jdbc:h2:~/test")
//                .option("driver", "org.h2.Driver")
//                .option("dbtable", tableName)
//                .option("user", "sa")
//                .option("password", "")
//                .mode(SaveMode.Overwrite)
//                .save();
//    }
}




//@Primary
//@Component
//public class ReadFile extends Job {
//
//    private final SparkSession sparkSession;
//
//    public ReadFile(SparkSession sparkSession) {
//        this.sparkSession = sparkSession;
//    }
//
//    @Override
//    public void preProcess() {
//        System.out.println("Pre-processing in ReadFile...");
//        test();
//    }
//
//    public void test() {
//        System.out.println("Test in ReadFile...");
//
//        // Get the file path from resources
//        URL resource = getClass().getClassLoader().getResource("xzy.txt");
//        System.out.println(resource);
//        if (resource == null) {
//            System.out.println("File not found in resources!");
//            return;
//        }
//
//        try {
//
//            // Convert URL to String Path
//            // String filePath = Paths.get(resource.getPath()).toString();
//            String filePath = Paths.get(resource.toURI()).toString();
//
//        // Read the text file as RDD
//        JavaRDD<String> lines = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD();
//
//        // Perform operations on the RDD
//        List<String> collectedLines = lines.collect();
//
//        // Print the content of the file
//        System.out.println("File content:");
//        collectedLines.forEach(System.out::println);
//
//        } catch (URISyntaxException e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//}

