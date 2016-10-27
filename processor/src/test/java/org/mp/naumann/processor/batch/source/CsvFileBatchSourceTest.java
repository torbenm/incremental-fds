package org.mp.naumann.processor.batch.source;


import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.handler.ConsoleOutputBatchHandler;
import org.testng.annotations.Test;

public class CsvFileBatchSourceTest{

        @Test
        public void test(){
            BatchSource bs = new CsvFileBatchSource("test.csv", "demotable", 2);
            BatchProcessor bp = new SynchronousBatchProcessor(bs);
            bp.addBatchHandler(new ConsoleOutputBatchHandler());
        }

}
