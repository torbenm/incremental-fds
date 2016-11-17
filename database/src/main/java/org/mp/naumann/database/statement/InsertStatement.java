package org.mp.naumann.database.statement;

public interface InsertStatement extends Statement {

    boolean isOfEqualSchema(InsertStatement statement);
}
