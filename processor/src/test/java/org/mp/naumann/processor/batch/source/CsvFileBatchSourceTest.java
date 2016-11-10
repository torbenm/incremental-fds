package org.mp.naumann.processor.batch.source;

import org.junit.Before;
import org.junit.Test;

public class CsvFileBatchSourceTest {

    CsvFileBatchSource csv;

    @Before
    public void init(){
        csv = new CsvFileBatchSource(
                getClass().getClassLoader().getResource("test.csv").getPath(),
                "", "demotable", 5
        );
    }


    @Test
    public void testCreateUpdateStatement(){

    }

}
