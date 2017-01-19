package org.mp.naumann.processor.batch.source;

import ResourceConnection.ResourceConnector;
import ResourceConnection.ResourceType;
import org.junit.Before;
import org.junit.Test;

public class CsvFileBatchSourceTest {

    CsvFileBatchSource csv;

    @Before
    public void init(){
        csv = new CsvFileBatchSource(
                ResourceConnector.getResourcePath(ResourceType.TEST, "test.csv"),
                "", "demotable", 5
        );
    }


    @Test
    public void testCreateUpdateStatement(){

    }

}
