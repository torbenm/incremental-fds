package org.mp.naumann;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.algorithms.result.PrintResultReceiver;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class Demo {

	private static final String TABLE = "test.countries";
	private static final int BATCH_SIZE = 10;
	private static final String SCHEMA = "test";

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		String file = Demo.class.getResource("countries.csv").getPath();
		StreamableBatchSource batchSource = new CsvFileBatchSource(file, SCHEMA, TABLE, BATCH_SIZE);
		DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
		BatchProcessor bp = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
		IncrementalAlgorithm<?, ?> batchHandler = new AverageIncrementalAlgorithm("population");
		batchHandler.addResultListener(new PrintResultReceiver<>());
		bp.addBatchHandler(batchHandler);
		batchSource.startStreaming();
	}

}
