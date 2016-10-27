package org.mp.naumann.processor.batch.source;

import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class SizableBatchSource extends AbstractBatchSource implements StreamableBatchSource{

    private final int batchSize;
    private final List<Statement> statementList = new ArrayList<Statement>();
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

    protected void weakStream(){
        if(hasEnoughToStream()){
            stream(batchSize);
            weakStream();
        }else if(doneFilling){
            forceStream();
        }
    }
    protected  void forceStream(){
        int size = hasEnoughToStream() ? batchSize : statementList.size() -currentStatementPosition;
        stream(size);
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

    public String getTableName() {
        return tableName;
    }

    protected int getCurrentStatementPosition() {
        return currentStatementPosition;
    }
}
