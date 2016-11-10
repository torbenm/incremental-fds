package org.mp.naumann.processor.batch.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.database.fake.FakeDeleteStatement;
import org.mp.naumann.processor.batch.source.helper.ExceptionThrowingBatchSourceListener;
import org.mp.naumann.processor.fake.FakeSizeableBatchSource;

public class SizableBatchSourceTest {

    private SizableBatchSource sbs;
    private static int BATCH_SIZE = 5;
    private static String TABLE_NAME = "demotable";


    @Before
    public void init() {
        sbs = new FakeSizeableBatchSource(TABLE_NAME, BATCH_SIZE);
    }

    @Test
    public void testGetBatchSize() {
        assertEquals(sbs.getBatchSize(), BATCH_SIZE);
    }

    @Test
    public void testGetTableName() {
        assertEquals(sbs.getTableName(), TABLE_NAME);
    }

    @Test
    public void testStartAndStopStreaming() {
        sbs.startStreaming();
        assertTrue(sbs.isStreaming());
        sbs.endStreaming();
        assertFalse(sbs.isStreaming());
    }

    @Test
    public void testAddStatement() {
        addStatement();
        assertTrue(sbs.hasSomethingToStream());
        assertEquals(sbs.hasEnoughToStream(), BATCH_SIZE == 1);
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testWeakStreaming() {
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        try {
            for (int i = 0; i < BATCH_SIZE - 1; i++) {
                addStatement();
                sbs.weakStream();
            }
        } catch (ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e) {
            throw new AssertionError();
        }
        addStatement();
        sbs.weakStream();
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testForceStream(){
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sbs.startStreaming();
        //GENERATE ONE LESS THAN NECESSARY TO TEST
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        assertFalse(sbs.hasEnoughToStream());
        sbs.forceStream();
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testFinishFilling(){
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sbs.startStreaming();
        //GENERATE ONE LESS THAN NECESSARY TO TEST
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        assertFalse(sbs.hasEnoughToStream());
        sbs.finishFilling();
    }

    @Test
    public void testEndStreaming(){
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sbs.startStreaming();
        boolean received = false;
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        try {
            sbs.endStreaming();
        }catch(ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e){
            received = true;
        }
        assertTrue(received);
        addStatement();
    }

    @Test
    public void testStartAndEndStreaming(){
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sbs.startStreaming();
        boolean received = false;
        int batches = 0;
        for (int i = 0; i < BATCH_SIZE*10 - 1; i++) {
            try {
                addStatement();
            }catch(ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e){
                batches++;
            }
        }
        try {
            sbs.endStreaming();
        }catch(ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e){
            received = true;
        }
        assertEquals(batches, 9);
        assertTrue(received);
    }

    @Test
    public void testNoStreamingWhenStreamingIsOff() {
        sbs.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        try {
            for (int i = 0; i < BATCH_SIZE * 10; i++) {
                addStatement();
            }
        } catch (ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e) {
            throw new AssertionError();
        }
    }

    @Test
    public void testHasEnoughToStream(){
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        assertFalse(sbs.hasEnoughToStream());
        addStatement();
        assertTrue(sbs.hasEnoughToStream());
    }

    
    private void addStatement(){
        sbs.addStatement(TABLE_NAME, new FakeDeleteStatement());
    }

}
