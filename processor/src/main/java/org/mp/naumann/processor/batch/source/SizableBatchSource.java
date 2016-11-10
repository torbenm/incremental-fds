package org.mp.naumann.processor.batch.source;

import java.util.ArrayList;
import java.util.List;

import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

public abstract class SizableBatchSource extends AbstractBatchSource implements StreamableBatchSource{

    private final int batchSize;
    private final List<Statement> statementList = new ArrayList<>();
    private final String tableName;
    private boolean streaming = false;
    private boolean doneFilling = false;
    private int currentStatementPosition = 0;

    public SizableBatchSource(String tableName, int batchSize) {
        this.batchSize = batchSize;
        this.tableName = tableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void startStreaming(){
        streaming = true;
        weakStream();
    }

    public void endStreaming(){
        //Stream one last time
        streaming = false;
        forceStream();
    }

    public boolean isStreaming(){
        return streaming;
    }

    public boolean isDoneFilling() {
        return doneFilling;
    }

    protected void finishFilling(){
        doneFilling = true;
        if(streaming)
            forceStream();
    }

    protected void addStatement(String tableName, Statement stmt){
        this.statementList.add(stmt);
        if(streaming)
            weakStream();
    }

    /**
     * Streams only when there is enough to stream. Calls itself afterwards again.
     * Otherwise, it checks if filling the storage up is done.
     * Then it calls forceStream to stream the rest.
     */
    protected void weakStream(){
        // Streams if either their is enough to fill a batch,
        // or all the rest if filling is completed.
        if(hasEnoughToStream()){
            stream(batchSize);
            weakStream();
        }else if(doneFilling){
            forceStream();
        }
    }

    /**
     * Streams either way. However, it does not send out batches bigger than the specified batch size,
     * but does not mind sending less either.
     */
    protected  void forceStream(){
        // Streams all there is left if it is fewer than the specified size
        if(hasSomethingToStream()){
            int size = hasEnoughToStream() ? batchSize : statementList.size() -currentStatementPosition;
            stream(size);
            forceStream();
        }
    }
    private synchronized void stream(int size){
        Batch batchToSend = new ListBatch(
                statementList.subList(currentStatementPosition, currentStatementPosition+size),
                this.tableName
        );
        currentStatementPosition += size;
        notifyListener(batchToSend);

    }

    protected boolean hasEnoughToStream(){
        return statementList.size() - currentStatementPosition >= batchSize;
    }

    protected boolean hasSomethingToStream(){
        return statementList.size() > currentStatementPosition;
    }

    public String getTableName() {
        return tableName;
    }

    protected int getCurrentStatementPosition() {
        return currentStatementPosition;
    }
}
