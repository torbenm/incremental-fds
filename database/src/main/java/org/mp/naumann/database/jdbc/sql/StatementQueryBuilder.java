package org.mp.naumann.database.jdbc.sql;

import org.mp.naumann.database.statement.Statement;

import java.util.List;

public interface StatementQueryBuilder<T extends Statement> {

    String generateSingle(T statement);
    String generateMulti(List<T> statements);

}
