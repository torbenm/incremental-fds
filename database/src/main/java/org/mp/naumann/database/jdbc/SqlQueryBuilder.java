package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

public class SqlQueryBuilder {

    public static String generateSql(Statement stmt){
        if(stmt instanceof InsertStatement){
            return generateSql((InsertStatement)stmt);
        }

        if(stmt instanceof DeleteStatement){
            return generateSql((DeleteStatement) stmt);
        }

        if(stmt instanceof UpdateStatement){
            return generateSql((UpdateStatement) stmt);
        }

        return "ERROR! However, this case should never occur.";
    }

    public static String generateSql(InsertStatement stmt){
        StringBuilder query = new StringBuilder("INSERT INTO ")
                .append(stmt.getTableName())
                .append(" (");

        return query.toString();
    }

    public static String generateSql(DeleteStatement stmt){
        StringBuilder query = new StringBuilder("DELETE FROM ")
                .append(stmt.getTableName())
                .append(" (");

        return query.toString();
    }

    public static String generateSql(UpdateStatement stmt){
        StringBuilder query = new StringBuilder("UPDATE ")
                .append(stmt.getTableName())
                .append(" (");

        return query.toString();
    }

}
