package org.mp.naumann.processor.handler.database;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.handler.BatchHandler;

public interface DatabaseBatchHandler extends BatchHandler {
	
	DataConnector getConnector();
}
