package com.example.FileProcessingSparkJava.service;

import com.example.FileProcessingSparkJava.job.Job;
import com.example.FileProcessingSparkJava.job.model.ColumnMetadata;
import com.example.FileProcessingSparkJava.job.read.ReadMetaData;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class ReadFile extends Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFile.class);

    private final ReadMetaData readMetaData;

    public ReadFile(ReadMetaData readMetaData) {
        this.readMetaData = readMetaData;
    }

//    @Override
//    public void preProcess() throws URISyntaxException, IOException {
//
//        LOGGER.info("Reading flat file...");
//        Dataset<Row> flatFileDataset =  readMetaData.readFlatFile("flatFile.txt");
//
//
//     //   LOGGER.info("Validating flat file...");
//    //    Dataset<Row> validatedDataset = validateData(flatFileDataset);
//
//        LOGGER.info("Reading metadata...");
//        List<ColumnMetadata> columnMetadataList = readMetaData.read("metaData.txt");
//
//        List<StructField> fields = new ArrayList<>();
//        columnMetadataList.forEach( data -> {
//            fields.add(DataTypes.createStructField(data.getColumnName(), DataTypes.StringType, true));
//        }  );
//
//        StructType schema = DataTypes.createStructType(fields);
//
//        LOGGER.info("Schema (as JSON):");
//        LOGGER.info(schema.prettyJson());
//
//
//
//
//        LOGGER.info("Storing validated data into H2 database...");
//       // storeToH2Database(validatedDataset, "validated_table");
//
//        LOGGER.info("Process ...");
//    }
//
//
//    private Dataset<Row> validateData(Dataset<Row> flatFileDataset, StructType schema) {
//        return flatFileDataset.filter((FilterFunction<Row>) row -> {
//            for (StructField field : schema.fields()) {
//                if (row.isNullAt(row.fieldIndex(field.name()))) {
//                    return false; // Reject rows with null values
//                }
//            }
//            return true;
//        });
//    }

    @Override
    public void preProcess() throws URISyntaxException, IOException {
        LOGGER.info("Reading flat file...");
        Dataset<Row> flatFileDataset = readMetaData.readFlatFile("flatFile.txt");

        LOGGER.info("Reading metadata...");
        List<ColumnMetadata> columnMetadataList = readMetaData.read("metaData.txt");

        LOGGER.info("Generating schema and extracting data...");
        Dataset<Row> transformedDataset = transformDataset(flatFileDataset, columnMetadataList);
        transformedDataset.show();
        transformedDataset.printSchema();
        LOGGER.info("Schema (as JSON):");
        LOGGER.info(transformedDataset.schema().prettyJson());

        LOGGER.info("Storing transformed data into H2 database...");
        storeToH2Database(transformedDataset, "student");

        LOGGER.info("Process completed successfully.");
    }


    /**
     * Transforms the dataset by extracting columns based on metadata.
     */


    private Dataset<Row> transformDataset(Dataset<Row> flatFileDataset, List<ColumnMetadata> columnMetadataList) {
        // Extract and add columns to the dataset based on metadata
        for (ColumnMetadata metadata : columnMetadataList) {
            String columnName = metadata.getColumnName();
            int start = metadata.getIndexStart();
            int end = metadata.getIndexEnd();

            // Calculate substring length (adjusting for 1-based indexing in Spark)
            int length = end - start;

            // Add the column using substring and trim function
            flatFileDataset = flatFileDataset.withColumn(
                    columnName,
                    functions.trim(functions.substring(flatFileDataset.col("value"), start + 1, length))
            );
        }

        // Log the length for each column in the dataset
        for (ColumnMetadata metadata : columnMetadataList) {
            String columnName = metadata.getColumnName();
            flatFileDataset = flatFileDataset.withColumn(
                    columnName + "_length",
                    functions.length(flatFileDataset.col(columnName))
            );
        }

        // Collect column names with '_length' appended to log their lengths
        List<Column> lengthColumns = columnMetadataList.stream()
                .map(metadata -> functions.col(metadata.getColumnName() + "_length")).collect(Collectors.toList());

        // Print the lengths of each column for debugging
        flatFileDataset.select(lengthColumns.toArray(new Column[0])).show();

        // Drop the original 'value' column and return the transformed dataset
        return flatFileDataset.drop("value");
    }


    /**
     * Stores the dataset into an H2 database table.
     */
    private void storeToH2Database(Dataset<Row> dataset, String tableName) {
        dataset.write()
                .format("jdbc")
                .option("url", "jdbc:h2:mem:testdb")
                .option("driver", "org.h2.Driver")
                .option("dbtable", tableName)
                .option("user", "sa")
                .option("password", "")
                .mode(SaveMode.Overwrite)
                .save();
    }


}
