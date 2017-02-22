package org.mp.naumann.processor.batch.source;

public class AbstractBatchSourceTest implements BatchSourceTest {


    private AbstractBatchSource abs;

    @Override
    public void testAddBatchSourceListener() {

    }

    @Override
    public void testRemoveBatchSourceListener() {

    }
/*
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
        assertEquals(2, abs.getBatchSourceListener().size());
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
        assertEquals(1, abs.getBatchSourceListener().size());
        assertFalse(abs.getBatchSourceListener().contains(fbsl));
        assertTrue(abs.getBatchSourceListener().contains(fbsl2));
    }

    @Test
    public void testNotifyListener(){
        for(int i = 0; i < 10; i++){
            abs.addBatchSourceListener(new FakeBatchSourceListener());
        }
        abs.notifyListener(new FakeBatch());
        assertEquals(0,
                abs.getBatchSourceListener()
                .parallelStream()
                .filter(n -> !((FakeBatchSourceListener)n).isReached())
                .count());
    } */
}
