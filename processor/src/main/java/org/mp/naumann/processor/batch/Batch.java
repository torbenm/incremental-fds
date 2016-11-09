package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.List;

public interface Batch extends Iterable<Statement>, StatementGroup<Statement> {

    int getSize();
    String getTableName();
}
