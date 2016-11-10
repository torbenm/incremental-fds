package org.mp.naumann;

import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.PassThroughDatabaseBatchHandler;

public class Demo {

	private static final String DRIVER_NAME = "org.postresql.Driver";
	private static final String HOST = "localhost";
	private static final int PORT = 5432;
	private static final String DATABASE = "postgres";
	private static final String PROTOCOL = "postgres";
	private static final String BATCH_FILE = null;
	private static final String TABLE = null;
	private static final int BATCH_SIZE = 10;

	public static void main(String[] args) {
		StreamableBatchSource batchSource = new CsvFileBatchSource(BATCH_FILE, TABLE, BATCH_SIZE);
		JdbcDataConnector dataConnector = new JdbcDataConnector(DRIVER_NAME,
				"jdbc:" + PROTOCOL + "//" + HOST + ":" + PORT + "/" + DATABASE);
		DatabaseBatchHandler databaseBatchHandler = new PassThroughDatabaseBatchHandler(dataConnector);
		BatchProcessor bp = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
		bp.addBatchHandler(null);
		batchSource.startStreaming();
	}

}
