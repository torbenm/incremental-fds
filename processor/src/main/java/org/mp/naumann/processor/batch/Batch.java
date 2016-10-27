package org.mp.naumann.processor.batch;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.List;

public interface Batch extends Iterable<Statement> {

    int getSize();
    List<Statement> getStatements();
    List<InsertStatement> getInsertStatements();
    List<DeleteStatement> getDeleteStatements();
    List<UpdateStatement> getUpdateStatements();

}
