package com.example.FileProcessingSparkJava.job.model;

public class ColumnMetadata {
    private String columnName;
    private String dataType;
    private int indexStart;
    private int indexEnd;

    public ColumnMetadata(String columnName, String dataType, int indexStart, int indexEnd) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.indexStart = indexStart;
        this.indexEnd = indexEnd;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getIndexStart() {
        return indexStart;
    }

    public void setIndexStart(int indexStart) {
        this.indexStart = indexStart;
    }

    public int getIndexEnd() {
        return indexEnd;
    }

    public void setIndexEnd(int indexEnd) {
        this.indexEnd = indexEnd;
    }
}
