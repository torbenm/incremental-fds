package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.VariableSizeBatchSource;
import org.mp.naumann.processor.handler.BatchHandler;

import java.sql.Connection;

public class VariableSizeBatches extends BaseTestCase {

    public VariableSizeBatches(String filename, IncrementalFDConfiguration config, int stopAfter) {
        super(filename, config, stopAfter);
    }

    protected String getBatchSize() {
        return "variable";
    }

    protected StreamableBatchSource getBatchSource(String schema, String tableName, int stopAfter) {
        return new VariableSizeBatchSource(schema, tableName, ResourceConnector.INSERTS + tableName);
    }

    protected Connection getCsvConnection() throws ConnectionException {
        return ConnectionManager.getCsvConnection(ResourceConnector.BASELINE, ",");
    }

    protected BatchHandler getInitialBatchHandler(Table table, HyFDInitialAlgorithm algorithm) {
        return null;
    }
}
