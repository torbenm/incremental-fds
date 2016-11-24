package org.mp.naumann.database.jdbc.sql.helper;

import java.util.HashMap;
import java.util.Map;

import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.UpdateStatement;

public class UpdateStatements {

    public static UpdateStatement createUpdateStatement1(){
        Map<String, String> newValues = new HashMap<>();
        newValues.put("name", "max");
        newValues.put("age", "15");

        Map<String, String> oldValues = new HashMap<>();
        oldValues.put("name", "hanna");
        oldValues.put("age", "12");

        return new DefaultUpdateStatement(newValues, oldValues, "test", "people");
    }

    public static UpdateStatement createUpdateStatementDifferentTable(){
        Map<String, String> newValues = new HashMap<>();
        newValues.put("city", "San Francisco");
        newValues.put("country", "US");

        Map<String, String> oldValues = new HashMap<>();
        oldValues.put("city", "Berlin");
        oldValues.put("country", "DE");

        return new DefaultUpdateStatement(newValues, oldValues, "test", "places");
    }
}
