package org.mp.naumann.database;


import org.mp.naumann.database.data.HasColumns;
import org.mp.naumann.database.data.HasName;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

public interface Table extends HasName, HasColumns<String> {

    long getRowCount();

    boolean execute(Statement statement);

    boolean execute(StatementGroup statementGroup);

    TableInput open() throws InputReadException;

    int getLimit();

    void setLimit(int limit);
}
