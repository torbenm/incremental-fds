package org.mp.naumann.processor.handler.database;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.handler.BatchHandler;

/**
 * Interface for {@link BatchHandler} which make sure that the database process the Batch.
 */
public interface DatabaseBatchHandler extends BatchHandler {

    /**
     * The database connection
     *
     * @return A DataConnector object representing the database connection
     */
    DataConnector getConnector();
}
