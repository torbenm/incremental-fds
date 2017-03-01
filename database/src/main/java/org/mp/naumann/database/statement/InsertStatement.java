package org.mp.naumann.database.statement;

import java.util.Map;

public interface InsertStatement extends Statement {

    Map<String, String> getValueMap();

    @Override
    default void accept(StatementVisitor visitor) {
        visitor.visit(this);
    }
}
