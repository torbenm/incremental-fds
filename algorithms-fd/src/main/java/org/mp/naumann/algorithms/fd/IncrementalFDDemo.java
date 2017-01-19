package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDVersion;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.net.URL;
import java.util.List;
import java.util.logging.Level;

public class IncrementalFDDemo {

      private static final String batchFileName = "csv/deletes.benchmark.adult.csv";
       private static final String schema = "";
       private static final String tableName = "benchmark.adult";
       private static final String csvDir = "/";
       private static final int batchSize = 1200; //*/
     /* private static final String batchFileName = "csv/deletes.deletesample.csv";
       private static final String schema = "";
       private static final String tableName = "test.deletesample";
       private static final String csvDir = "/";
       private static final int batchSize = 100;  //*/
  /* private static final String batchFileName = "csv/test/deletes.bridges.csv";
    private static final String schema = "";
    private static final String csvDir = "/test";
    private static final String tableName = "test.bridges";
    private static final int batchSize = 200; //*/

    public static void main(String[] args) throws ClassNotFoundException, ConnectionException, AlgorithmExecutionException {
        FDLogger.setLevel(Level.INFO);
        try (DataConnector dc = new JdbcDataConnector(
                ConnectionManager.getCsvConnection(IncrementalFDDemo.class, csvDir, ","))) {

            // execute initial algorithm
            Table table = dc.getTable(schema, tableName);
            HyFDInitialAlgorithm hyfd = new HyFDInitialAlgorithm(table);
            List<FunctionalDependency> fds = hyfd.execute();
            FDLogger.log(Level.INFO, String.format("Original FD count: %s", fds.size()));
            FDLogger.log(Level.INFO, String.format("Batch size: %s", batchSize));
            FDLogger.log(Level.FINEST, "\n");
            fds.forEach(fd -> FDLogger.log(Level.FINEST, fd.toString()));
            FDIntermediateDatastructure ds = hyfd.getIntermediateDataStructure();

            // create batch source & processor for inserts
            URL res = IncrementalFDDemo.class.getClassLoader().getResource(batchFileName);
            if (res == null)
                throw new RuntimeException("Couldn't find csv file for batches.");
            StreamableBatchSource batchSource = new CsvFileBatchSource(res.getFile(), schema, tableName, batchSize);
            DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
            BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

            // create incremental algorithm
            IncrementalFD algorithm = new IncrementalFD(table.getColumnNames(), tableName, IncrementalFDVersion.LATEST);
            IncrementalFDResultListener listener = new IncrementalFDResultListener();
            algorithm.addResultListener(listener);
            algorithm.setIntermediateDataStructure(ds);

            // process batch
            batchProcessor.addBatchHandler(algorithm);
            batchSource.startStreaming();

            // output results
            FDLogger.log(Level.INFO, String.format("Total performed validations: %s", listener.getValidationCount()));
            FDLogger.log(Level.INFO, String.format("Total pruned validations: %s", listener.getPrunedCount()));
            FDLogger.log(Level.INFO, String.format("Final FD count: %s", listener.getFDs().size()));
            listener.getFDs().forEach(f -> FDLogger.log(Level.INFO, f.toString()));
        }
    }

}
