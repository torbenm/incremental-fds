package org.mp.naumann.database.fake;

import org.mp.naumann.database.Table;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.data.Row;
import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifierGroup;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

import java.util.List;

public class FakeTable implements Table {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> getColumnNames() {
        return null;
    }

    @Override
    public long getRowCount() {
        return 0;
    }

    @Override
    public Row getRow(RowIdentifier rowIdentifier) {
        return null;
    }

    @Override
    public Column getColumn(String name) {
        return null;
    }

    @Override
    public boolean execute(Statement statement) {
        return false;
    }

    @Override
    public boolean execute(StatementGroup statementGroup) {
        throw new RuntimeException();
    }

    @Override
    public Table getSubTable(RowIdentifierGroup group) {
        return null;
    }

    @Override
    public Class<? extends RowIdentifier> getRowIdentifierType() {
        return null;
    }
}
