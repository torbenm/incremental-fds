package org.mp.naumann.database.statement;

import java.util.List;

public interface StatementGroup<T extends Statement> {

    List<T> getStatements();
}
