
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.SizableBatchSource;
import org.mp.naumann.processor.handler.ConsoleOutputBatchHandler;
import org.testng.annotations.Test;

public class DemoCSV {

    @Test
    public void demo(){
        String path = "/Users/torbenmeyer/Development/hpi/mp/incremental-fds/processor/src/test/java/test.csv";
        SizableBatchSource bs = new CsvFileBatchSource(path, "demotable", 10);
        BatchProcessor bp = new SynchronousBatchProcessor(bs);
        bp.addBatchHandler(new ConsoleOutputBatchHandler());
        bs.startStreaming();
    }

}
