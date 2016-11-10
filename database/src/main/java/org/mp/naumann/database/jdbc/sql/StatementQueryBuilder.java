package org.mp.naumann.database.jdbc.sql;

import java.util.List;

import org.mp.naumann.database.statement.Statement;

public interface StatementQueryBuilder<T extends Statement> {

    String generateSingle(T statement);
    String generateMulti(List<T> statements);

}
