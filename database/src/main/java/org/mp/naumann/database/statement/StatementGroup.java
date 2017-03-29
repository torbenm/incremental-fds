package org.mp.naumann.database.statement;

import java.util.List;

public interface StatementGroup {

    List<Statement> getStatements();

    List<InsertStatement> getInsertStatements();

    List<DeleteStatement> getDeleteStatements();

    List<UpdateStatement> getUpdateStatements();
}
