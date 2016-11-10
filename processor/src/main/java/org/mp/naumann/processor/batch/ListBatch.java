package org.mp.naumann.processor.batch;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.UpdateStatement;

public class ListBatch implements Batch {

    private final List<Statement> statements;
    private final String tableName;

    public ListBatch(List<Statement> statements, String tableName) {
        this.statements = statements;
        this.tableName = tableName;
    }

    @Override
    public int getSize() {
        return statements.size();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<InsertStatement> getInsertStatements() {
        return filterStatements(InsertStatement.class);
    }

    @Override
    public List<DeleteStatement> getDeleteStatements() {
        return filterStatements(DeleteStatement.class);
    }

    @Override
    public List<UpdateStatement> getUpdateStatements() {
        return filterStatements(UpdateStatement.class);
    }

    private <T extends Statement> List<T> filterStatements(final Class<T> clazz){
        return getStatements().parallelStream()
                .filter(clazz::isInstance)
                .map(n -> (T)n)
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<Statement> iterator() {
        return getStatements().iterator();
    }

    @Override
    public List<Statement> getStatements() {
        return statements;
    }
}
