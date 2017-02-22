package org.mp.naumann;

import org.mockito.Mockito;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageDatastructure;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.algorithms.result.PrintResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.FixedSizeCsvBatchSource;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import ResourceConnection.ResourceConnector;

public class Demo {

	private static final String TABLE = "countries";
	private static final int BATCH_SIZE = 10;
	private static final String SCHEMA = "";

	public static void main(String[] args) throws ClassNotFoundException, ConnectionException {
		String file = ResourceConnector.getResourcePath(ResourceConnector.FULL_BATCHES, "inserts.countries.csv");
		StreamableBatchSource batchSource = new FixedSizeCsvBatchSource(file, SCHEMA, TABLE, BATCH_SIZE);
		DatabaseBatchHandler databaseBatchHandler = Mockito.mock(DatabaseBatchHandler.class);
		BatchProcessor bp = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

		AverageDatastructure popDs = new AverageDatastructure();
		AverageDatastructure areaDs = new AverageDatastructure();

		IncrementalAlgorithm<Double, AverageDatastructure> popAvg = new AverageIncrementalAlgorithm("population");
		IncrementalAlgorithm<Double, AverageDatastructure> areaAvg = new AverageIncrementalAlgorithm("area");
		popAvg.initialize(popDs);
		areaAvg.initialize(areaDs);
		popAvg.addResultListener(new PrintResultListener<>("population"));
		areaAvg.addResultListener(new PrintResultListener<>("area"));
		bp.addBatchHandler(popAvg);
		bp.addBatchHandler(areaAvg);
		batchSource.startStreaming();
	}

}
