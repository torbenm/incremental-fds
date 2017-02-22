package org.mp.naumann.processor.batch.source;

import org.junit.Test;
import org.mockito.Mockito;
import org.mp.naumann.processor.batch.Batch;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class AbstractBatchSourceTest {

    protected AbstractBatchSource abstractBatchSource;

    protected int numberOfBatchSourceListeners = 20;

    @Test
    public void test_add_remove_batchsource_listener(){
        BatchSourceListener bsl1 = mock(BatchSourceListener.class);
        BatchSourceListener bsl2 = mock(BatchSourceListener.class);

        abstractBatchSource.addBatchSourceListener(bsl1);
        abstractBatchSource.addBatchSourceListener(bsl2);

        assertEquals(2, abstractBatchSource.getBatchSourceListener().size());
        assertTrue(abstractBatchSource.getBatchSourceListener().contains(bsl1));
        assertTrue(abstractBatchSource.getBatchSourceListener().contains(bsl2));

        abstractBatchSource.removeBatchSourceListener(bsl1);
        assertEquals(1, abstractBatchSource.getBatchSourceListener().size());
        assertFalse(abstractBatchSource.getBatchSourceListener().contains(bsl1));
        assertTrue(abstractBatchSource.getBatchSourceListener().contains(bsl2));

        abstractBatchSource.removeBatchSourceListener(bsl2);

    }

    @Test
    public void testNotifyListener(){
        Batch batch = mock(Batch.class);
        List<BatchSourceListener> batchSourceListeners = new ArrayList<>(numberOfBatchSourceListeners);
        for(int i = 0; i < numberOfBatchSourceListeners; i++){
            batchSourceListeners.add(mock(BatchSourceListener.class));
            abstractBatchSource.addBatchSourceListener(batchSourceListeners.get(i));
        }
        abstractBatchSource.notifyListener(batch);

        batchSourceListeners.stream().map(Mockito::verify).forEach(b -> b.batchArrived(batch));
        batchSourceListeners.forEach(abstractBatchSource::removeBatchSourceListener);
    }
}
