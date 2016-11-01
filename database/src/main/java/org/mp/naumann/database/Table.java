package org.mp.naumann.database;


import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifierGroup;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

public interface Table extends HasName, HasColumns {

    long getRowCount();
    Row getRow(RowIdentifier rowIdentifier);

    Column getColumn(String name);

    boolean execute(Statement statement);
    boolean execute(StatementGroup statementGroup);

    Table getSubTable(RowIdentifierGroup group);

    Class<? extends RowIdentifier> getRowIdentifierType();

}
