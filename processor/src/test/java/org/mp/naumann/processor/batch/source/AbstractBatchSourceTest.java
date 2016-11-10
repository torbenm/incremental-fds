package org.mp.naumann.processor.batch.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.processor.fake.FakeAbstractBatchSource;
import org.mp.naumann.processor.fake.FakeBatch;
import org.mp.naumann.processor.fake.FakeBatchSourceListener;

public class AbstractBatchSourceTest implements BatchSourceTest {


    private AbstractBatchSource abs;

    @Before
    public void init(){
        abs = new FakeAbstractBatchSource();
    }


    @Override
    public void testAddBatchSourceListener() {
        FakeBatchSourceListener fbsl = new FakeBatchSourceListener();
        FakeBatchSourceListener fbsl2 = new FakeBatchSourceListener();
        abs.addBatchSourceListener(fbsl);
        abs.addBatchSourceListener(fbsl2);
        assertEquals(abs.getBatchSourceListener().size(), 2);
        assertTrue(abs.getBatchSourceListener().contains(fbsl));
        assertTrue(abs.getBatchSourceListener().contains(fbsl2));
    }

    @Override
    public void testRemoveBatchSourceListener() {
        FakeBatchSourceListener fbsl = new FakeBatchSourceListener();
        FakeBatchSourceListener fbsl2 = new FakeBatchSourceListener();
        abs.addBatchSourceListener(fbsl);
        abs.addBatchSourceListener(fbsl2);
        abs.removeBatchSourceListener(fbsl);
        assertEquals(abs.getBatchSourceListener().size(), 1);
        assertFalse(abs.getBatchSourceListener().contains(fbsl));
        assertTrue(abs.getBatchSourceListener().contains(fbsl2));
    }

    @Test
    public void testNotifyListener(){
        for(int i = 0; i < 10; i++){
            abs.addBatchSourceListener(new FakeBatchSourceListener());
        }
        abs.notifyListener(new FakeBatch());
        assertEquals(abs.getBatchSourceListener()
                .parallelStream()
                .filter(n -> !((FakeBatchSourceListener)n).isReached())
                .count(), 0);
    }
}
