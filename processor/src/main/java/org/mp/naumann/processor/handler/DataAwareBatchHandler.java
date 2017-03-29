package org.mp.naumann.processor.handler;

import org.mp.naumann.database.DataConnector;

/**
 * A DataAwareBatchHandler is a BatchHandler that is aware of the database in the background.
 */
public interface DataAwareBatchHandler extends BatchHandler {

    /**
     * Establishes the connection of this BatchHandler to the database in the background.
     *
     * @param dataConnector A DataConnector object representing the database connection.
     */
    void setDataConnector(DataConnector dataConnector);

}
