package org.mp.naumann.database.jdbc.sql.helper;

import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.identifier.EmptyRowIdentifier;
import org.mp.naumann.database.statement.DefaultDeleteStatement;

import java.util.HashMap;
import java.util.Map;

public class DeleteStatements {


    public static DeleteStatement createDeleteStatement1(){
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("country", "DE");
        valueMap.put("city", "Berlin");
        valueMap.put("street", "Unter den Linden");
        return new DefaultDeleteStatement(valueMap, new EmptyRowIdentifier(), "places");
    }

    public static DeleteStatement createDeleteStatement2(){
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("country", "DE");
        valueMap.put("city", "Potsdam");
        valueMap.put("street", "August-Bebel-Str.");
        return new DefaultDeleteStatement(valueMap, new EmptyRowIdentifier(), "places");
    }
    public static DeleteStatement createDeleteStatement2Columns(){
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("country", "US");
        valueMap.put("city", "San Francisco");
        return new DefaultDeleteStatement(valueMap, new EmptyRowIdentifier(), "places");
    }

    public static DeleteStatement createDeleteStatementOtherTable(){
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("name", "Max");
        valueMap.put("age", "15");
        return new DefaultDeleteStatement(valueMap, new EmptyRowIdentifier(), "persons");
    }

    public static DeleteStatement createDeleteStatementEmptyValueMap(){
        Map<String, String> valueMap = new HashMap<>();
        return new DefaultDeleteStatement(valueMap, new EmptyRowIdentifier(), "places");
    }

}
