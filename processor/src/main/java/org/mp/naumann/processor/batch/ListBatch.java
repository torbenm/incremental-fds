package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementVisitorAdapter;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of a {@link Batch} which stores the statements in a List.
 */
public class ListBatch implements Batch {

    private final List<Statement> statements;
    private final String schema, tableName;

    public ListBatch(List<Statement> statements, String schema, String tableName) {
        this.schema = schema;
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

    public String getSchema() {
        return schema;
    }

    @Override
    public List<InsertStatement> getInsertStatements() {
        InsertCollector collector = new InsertCollector();
        statements.forEach(stmt -> stmt.accept(collector));
        return collector.getStatements();
    }

    @Override
    public List<DeleteStatement> getDeleteStatements() {
        DeleteCollector collector = new DeleteCollector();
        statements.forEach(stmt -> stmt.accept(collector));
        return collector.getStatements();
    }

    @Override
    public List<UpdateStatement> getUpdateStatements() {
        UpdateCollector collector = new UpdateCollector();
        statements.forEach(stmt -> stmt.accept(collector));
        return collector.getStatements();
    }

    @Override
    public Iterator<Statement> iterator() {
        return getStatements().iterator();
    }

    @Override
    public List<Statement> getStatements() {
        return statements;
    }

    private static class UpdateCollector extends StatementCollector<UpdateStatement> {

        @Override
        public void visit(UpdateStatement update) {
            statements.add(update);
        }
    }

    private static class DeleteCollector extends StatementCollector<DeleteStatement> {

        @Override
        public void visit(DeleteStatement delete) {
            statements.add(delete);
        }
    }

    private static class InsertCollector extends StatementCollector<InsertStatement> {

        @Override
        public void visit(InsertStatement inserts) {
            statements.add(inserts);
        }
    }

    private static abstract class StatementCollector<T extends Statement> extends StatementVisitorAdapter {

        List<T> statements = new ArrayList<>();

        List<T> getStatements() {
            return statements;
        }
    }
}
