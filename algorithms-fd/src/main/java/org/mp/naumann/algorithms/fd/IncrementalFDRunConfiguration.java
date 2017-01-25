package org.mp.naumann.algorithms.fd;

import ResourceConnection.ResourceType;

public class IncrementalFDRunConfiguration {


    private final String batchFileName;
    private final String schema;
    private final String tableName;
    private final int batchSize;
    private final ResourceType resourceType;
    private final String separator;
    public IncrementalFDRunConfiguration(String batchFileName, String schema, String tableName, int batchSize, ResourceType resourceType, String separator) {
        this.batchFileName = batchFileName;
        this.schema = schema;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.resourceType = resourceType;
        this.separator = separator;
    }

    public String getSeparator() {
        return separator;
    }

    public String getBatchFileName() {
        return batchFileName;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }
}
