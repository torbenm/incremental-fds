package org.mp.naumann.processor.batch;


import static org.junit.Assert.assertEquals;

public class BatchTest {


	public void checkBatchSize(Batch b){
        assertEquals(
                b.getInsertStatements().size() +
                        b.getDeleteStatements().size() +
                        b.getUpdateStatements().size(),
                b.getSize()
        );
    }


}
