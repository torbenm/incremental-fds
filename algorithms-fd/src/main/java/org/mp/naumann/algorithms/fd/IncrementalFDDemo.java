package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.List;
import java.util.logging.Level;

import ResourceConnection.ResourceConnector;
import ResourceConnection.ResourceType;

public class IncrementalFDDemo {

      private static final String batchFileName = "simple.csv";
       private static final String schema = "";
       private static final String tableName = "simple";
       private static final int batchSize = 200;
    private static final ResourceType resourceType = ResourceType.BASELINE;//*/
  /*    private static final String batchFileName = "deletes.deletesample.csv";
       private static final String schema = "";
       private static final String tableName = "test.deletesample";
    private static final ResourceType resourceType = ResourceType.TEST;
       private static final String csvDir = "/";
       private static final int batchSize = 100;  //*/
  /* private static final String batchFileName = "deletes.bridges.csv";
    private static final String schema = "";
   // private static final String csvDir = "/test";
    private static final String tableName = "test.bridges";
    private static final ResourceType resourceType = ResourceType.TEST;
    private static final int batchSize = 200; //*/

    public static void main(String[] args) throws ClassNotFoundException, ConnectionException, AlgorithmExecutionException {
        FDLogger.setLevel(Level.FINE);
        IncrementalFDConfiguration configuration = new IncrementalFDConfiguration("custom").addPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION);
        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(f -> {
                    if(f.getLevel() == BenchmarkLevel.UNIQUE) System.out.println(f);
        }
        );

        try (DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection(resourceType, ","))) {

            // execute initial algorithm
            Table table = dc.getTable(schema, tableName);
            HyFDInitialAlgorithm hyfd = new HyFDInitialAlgorithm(configuration, table);
            List<FunctionalDependency> fds = hyfd.execute();
            FDLogger.log(Level.INFO, String.format("Original FD count: %s", fds.size()));
            FDLogger.log(Level.INFO, String.format("Batch size: %s", batchSize));
            FDLogger.log(Level.FINEST, "\n");
            fds.forEach(fd -> FDLogger.log(Level.FINEST, fd.toString()));
            FDIntermediateDatastructure ds = hyfd.getIntermediateDataStructure();

            // create batch source & processor for inserts
            String batchFile = ResourceConnector.getResourcePath(ResourceType.FULL_BATCHES, batchFileName);
            StreamableBatchSource batchSource = new FixedSizeBatchSource(batchFile, schema, tableName, batchSize);
            DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
            BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

            // create incremental algorithm
            SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
            IncrementalFD algorithm = new IncrementalFD(table.getColumnNames(), tableName, configuration);
            IncrementalFDResultListener listener = new IncrementalFDResultListener();
            algorithm.addResultListener(listener);
            algorithm.setIntermediateDataStructure(ds);

            // process batch
            batchProcessor.addBatchHandler(algorithm);
            batchSource.startStreaming();
            SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished processing 1 batch");

            // output results
            FDLogger.log(Level.INFO, String.format("Total performed validations: %s", listener.getValidationCount()));
            FDLogger.log(Level.INFO, String.format("Total pruned validations: %s", listener.getPrunedCount()));
            FDLogger.log(Level.INFO, String.format("Final FD count: %s", listener.getFDs().size()));
            listener.getFDs().forEach(f -> FDLogger.log(Level.FINE, f.toString()));
        }
    }

}
