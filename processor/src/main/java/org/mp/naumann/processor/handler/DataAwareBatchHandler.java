package org.mp.naumann.processor.handler;

import org.mp.naumann.database.DataConnector;

public interface DataAwareBatchHandler extends BatchHandler {

	void setDataConnector(DataConnector dataConnector);

}
