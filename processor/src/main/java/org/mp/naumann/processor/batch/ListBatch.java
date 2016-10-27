package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.Iterator;
import java.util.List;

public class ListBatch implements Batch {

    private final List<Statement> statements;

    public ListBatch(List<Statement> statements, String tableName) {
        this.statements = statements;
    }

    @Override
    public int getSize() {
        return statements.size();
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public List<InsertStatement> getInsertStatements() {
        return null;
    }

    @Override
    public List<DeleteStatement> getDeleteStatements() {
        return null;
    }

    @Override
    public List<UpdateStatement> getUpdateStatements() {
        return null;
    }

    @Override
    public Iterator<Statement> iterator() {
        return getStatements().iterator();
    }

    @Override
    public List<Statement> getStatements() {
        return null;
    }
}
