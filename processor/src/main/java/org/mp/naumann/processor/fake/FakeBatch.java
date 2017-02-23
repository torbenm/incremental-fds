package org.mp.naumann.processor.fake;

import java.util.Iterator;
import java.util.List;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

public class FakeBatch implements Batch {
    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public String getTableName() {
        return null;
    }

    public String getSchema() { return null; }

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
        return null;
    }

    @Override
    public List<Statement> getStatements() {
        return null;
    }
}