package org.mp.naumann.processor.batch.source.csv;


import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;


public abstract class SizableCsvBatchSource extends CsvFileBatchSource implements StreamableBatchSource {

    private final int batchSize;
    private boolean streaming = false;
    private boolean doneFilling = false;
    private int currentStatementPosition = 0;
    private int stopAfter = -1;
    private int currentBatch = 0;

    public SizableCsvBatchSource(String schema, String tableName, int batchSize) {
        super(schema, tableName);
        this.batchSize = batchSize;
    }

    public SizableCsvBatchSource(String schema, String tableName, int batchSize, int stopAfter, int skipFirst) {
        this(schema, tableName, batchSize);
        this.stopAfter = stopAfter;
        this.currentStatementPosition = skipFirst;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void startStreaming() {
        streaming = true;
        weakStream();
        start();
    }

    protected abstract void start();

    public void endStreaming() {
        //Stream one last time
        streaming = false;
        forceStream();
    }

    public boolean isStreaming() {
        return streaming;
    }

    public boolean isDoneFilling() {
        return doneFilling;
    }

    protected void finishFilling() {
        doneFilling = true;
        if (streaming)
            forceStream();
    }

    protected void addStatement(Statement stmt) {
        this.statementList.add(stmt);
        if (streaming)
            weakStream();
    }

    /**
     * Streams only when there is enough to stream. Calls itself afterwards again.
     * Otherwise, it checks if filling the storage up is done.
     * Then it calls forceStream to stream the rest.
     */
    protected void weakStream() {
        // Streams if either their is enough to fill a batch,
        // or all the rest if filling is completed.
        if (hasEnoughToStream() && streaming) {
            stream(batchSize);
            weakStream();
        } else if (doneFilling) {
            forceStream();
        }
    }

    /**
     * Streams either way. However, it does not send out batches bigger than the specified batch
     * size, but does not mind sending less either.
     */
    protected void forceStream() {
        // Streams all there is left if it is fewer than the specified size
        if (hasSomethingToStream()) {
            int size = hasEnoughToStream() ? batchSize : statementList.size() - currentStatementPosition;
            stream(size);
            forceStream();
        }
    }

    private synchronized void stream(int size) {
        if (stopAfter < 0 || currentBatch < stopAfter) {
            Batch batchToSend = new ListBatch(
                    statementList.subList(currentStatementPosition, currentStatementPosition + size),
                    this.schema,
                    this.tableName
            );
            currentStatementPosition += size;
            notifyListener(batchToSend);
            currentBatch++;
        } else {
            streaming = false;
        }

    }

    protected boolean hasEnoughToStream() {
        return statementList.size() - currentStatementPosition >= batchSize;
    }

    protected boolean hasSomethingToStream() {
        return statementList.size() > currentStatementPosition;
    }

    protected int getCurrentStatementPosition() {
        return currentStatementPosition;
    }
}
