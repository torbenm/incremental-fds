package org.mp.naumann.processor.batch;


import static org.junit.Assert.assertEquals;

public abstract class BatchTest {


    public void checkBatchSize(Batch batch) {
        assertEquals(
                batch.getInsertStatements().size() +
                        batch.getDeleteStatements().size() +
                        batch.getUpdateStatements().size(),
                batch.getSize()
        );
    }


}
