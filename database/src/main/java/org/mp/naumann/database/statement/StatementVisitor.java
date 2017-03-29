package org.mp.naumann.database.statement;

public interface StatementVisitor {
    void visit(DeleteStatement delete);

    void visit(UpdateStatement update);

    void visit(InsertStatement insert);
}
