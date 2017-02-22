package org.mp.naumann.processor.batch.source.csv;

import org.junit.Test;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.source.AbstractBatchSourceTest;
import org.mp.naumann.processor.batch.source.helper.ExceptionThrowingBatchSourceListener;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SizableCsvBatchSourceTest extends AbstractBatchSourceTest {

    private SizableCsvBatchSource sizableCsvBatchSource;
    private final static int BATCH_SIZE = 5;
    private final static String TABLE_NAME = "demotable";
    private final static String SCHEMA = "";


    public SizableCsvBatchSourceTest(){
        this.sizableCsvBatchSource = new SizableCsvBatchSource(SCHEMA, TABLE_NAME, BATCH_SIZE) {
            @Override
            protected void start() {

            }
        };
        this.abstractBatchSource = this.sizableCsvBatchSource;
    }

    @Test
    public void testGetBatchSize() {
        assertEquals(BATCH_SIZE, sizableCsvBatchSource.getBatchSize());
    }

    @Test
    public void testStartAndStopStreaming() {
        sizableCsvBatchSource.startStreaming();
        assertTrue(sizableCsvBatchSource.isStreaming());
        sizableCsvBatchSource.endStreaming();
        assertFalse(sizableCsvBatchSource.isStreaming());
    }

    @Test
    public void testAddStatement() {
        addStatement();
        assertTrue(sizableCsvBatchSource.hasSomethingToStream());
        assertEquals(BATCH_SIZE == 1, sizableCsvBatchSource.hasEnoughToStream());
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testWeakStreaming() {
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sizableCsvBatchSource.startStreaming();
        try {
            for (int i = 0; i < BATCH_SIZE - 1; i++) {
                addStatement();
                sizableCsvBatchSource.weakStream();
            }
        } catch (ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e) {
            throw new AssertionError();
        }
        addStatement();
        sizableCsvBatchSource.weakStream();
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testForceStream(){
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sizableCsvBatchSource.startStreaming();
        //GENERATE ONE LESS THAN NECESSARY TO TEST
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        assertFalse(sizableCsvBatchSource.hasEnoughToStream());
        sizableCsvBatchSource.forceStream();
    }

    @Test(expected= ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException.class)
    public void testFinishFilling(){
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sizableCsvBatchSource.startStreaming();
        //GENERATE ONE LESS THAN NECESSARY TO TEST
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        assertFalse(sizableCsvBatchSource.hasEnoughToStream());
        sizableCsvBatchSource.finishFilling();
    }

    @Test
    public void testEndStreaming(){
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sizableCsvBatchSource.startStreaming();
        boolean received = false;
        for (int i = 0; i < BATCH_SIZE - 1; i++) {
            addStatement();
        }
        try {
            sizableCsvBatchSource.endStreaming();
        }catch(ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e){
            received = true;
        }
        assertTrue(received);
        addStatement();
    }

    @Test
    public void testStartAndEndStreaming(){
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
        sizableCsvBatchSource.startStreaming();
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
            sizableCsvBatchSource.endStreaming();
        }catch(ExceptionThrowingBatchSourceListener.ExceptionThrowingBatchSourceListenerException e){
            received = true;
        }
        assertEquals(9, batches);
        assertTrue(received);
    }

    @Test
    public void testNoStreamingWhenStreamingIsOff() {
        sizableCsvBatchSource.addBatchSourceListener(new ExceptionThrowingBatchSourceListener());
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
        assertFalse(sizableCsvBatchSource.hasEnoughToStream());
        addStatement();
        assertTrue(sizableCsvBatchSource.hasEnoughToStream());
    }
    
    
    private void addStatement(){

        sizableCsvBatchSource.addStatement(mock(Statement.class));
    }

}
