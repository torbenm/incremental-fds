package org.mp.naumann.processor.batch.source;

import org.junit.Before;
import org.junit.Test;

import ResourceConnection.ResourceConnector;

public class FixedSizeBatchSourceTest {

    FixedSizeBatchSource csv;

    @Before
    public void init(){
        csv = new FixedSizeBatchSource(
                ResourceConnector.getResourcePath(ResourceConnector.TEST, "test.csv"),
                "", "demotable", 5
        );
    }


    @Test
    public void testCreateUpdateStatement(){

    }

}
