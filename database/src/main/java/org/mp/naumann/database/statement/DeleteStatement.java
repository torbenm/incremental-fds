package org.mp.naumann.database.statement;

public interface DeleteStatement extends Statement {


    static boolean areEqualSchema(DeleteStatement stmt1, DeleteStatement stmt2){
        return stmt1.getTableName().equalsIgnoreCase(stmt2.getTableName());
    }


}
