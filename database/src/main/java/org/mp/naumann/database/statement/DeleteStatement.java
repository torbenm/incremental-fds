package org.mp.naumann.database.statement;

public interface DeleteStatement extends Statement {


    default boolean isOfEqualSchema(DeleteStatement statement){
        return this.getTableName().equalsIgnoreCase(statement.getTableName());
    }


}
