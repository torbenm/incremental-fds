package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

public interface Batch extends Iterable<Statement>, StatementGroup {

    int getSize();
    String getTableName();
}
