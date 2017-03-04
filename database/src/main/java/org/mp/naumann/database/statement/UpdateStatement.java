package org.mp.naumann.database.statement;

import java.util.Map;

public interface UpdateStatement extends Statement {

    Map<String, String> getOldValueMap();

    Map<String, String> getValueMap();

    @Override
    default void accept(StatementVisitor visitor) {
        visitor.visit(this);
    }
}
