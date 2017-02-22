package org.mp.naumann.processor.batch.source.csv;

import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

import java.io.File;

public class VariableSizeCsvBatchSource extends CsvFileBatchSource implements StreamableBatchSource {

    private final String directory;
    private boolean streaming = false;

    public VariableSizeCsvBatchSource(String schema, String table, String directory) {
        super(schema, table);
        this.directory = directory;
    }

    public void startStreaming() {
        streaming = true;
        int currentBatch = 1;
        while (streamFileAsBatch(currentBatch) && streaming) currentBatch++;
    }

    private boolean streamFileAsBatch(int fileNumber) {
        File file = new File(ResourceConnector.getResourcePath(String.format("%s/%s.csv", directory, fileNumber)));
        if (file.exists()) {
            statementList.clear();
            parseFile(file);
            Batch batchToSend = new ListBatch(statementList, schema, tableName);
            notifyListener(batchToSend);
        }
        return file.exists();
    }

    public void endStreaming() {
        streaming = false;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public boolean isDoneFilling() {
        return true;
    }

    void addStatement(Statement stmt) {
        statementList.add(stmt);
    }
}
