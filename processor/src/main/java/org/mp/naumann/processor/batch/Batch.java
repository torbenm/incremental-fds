package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;


/**
 * A Batch is a Collection of {@link Statement} which come from a {@link
 * org.mp.naumann.processor.batch.source.BatchSource} It always consists of at least one statement,
 * and statements can be * {@link org.mp.naumann.database.statement.DeleteStatement} * {@link
 * org.mp.naumann.database.statement.InsertStatement} * {@link org.mp.naumann.database.statement.UpdateStatement}
 *
 * Furthermore, a batch must also uniquely identify a table in a database through a table name and a
 * schema.
 */
public interface Batch extends Iterable<Statement>, StatementGroup {

    /**
     * The size of the batch.
     *
     * @return The size of the batch
     */
    int getSize();

    /**
     * The table this batch is referring to.
     *
     * @return The table name as a {@link String}
     */
    String getTableName();

    /**
     * The schema of the table this batch is referring to.
     *
     * @return The schema name as a {@link String}
     */
    String getSchema();
}
