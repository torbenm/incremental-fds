package org.mp.naumann.database.jdbc.sql.helper;

import org.mp.naumann.database.identifier.EmptyRowIdentifier;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.InsertStatement;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class InsertStatements {

    public static InsertStatement createPeopleInsertWrongTable(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "max");
        insertValues.put("age", "15");
        insertValues.put("birthday", "2016-11-01");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "persons"
        );
    }


    public static InsertStatement createPeopleInsert1(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "max");
        insertValues.put("age", "15");
        insertValues.put("birthday", "2016-11-01");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "people"
        );
    }

    public static InsertStatement createPeopleInsert2(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "hanna");
        insertValues.put("age", "29");
        insertValues.put("birthday", "2014-12-03");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "people"
        );
    }

    public static InsertStatement createPeopleInsert3(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "frieda");
        insertValues.put("age", "1029");
        insertValues.put("birthday", "1024-02-02");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "people"
        );
    }

    public static InsertStatement createPeopleInsertEmpty(){
        return new DefaultInsertStatement(new HashMap<>(), new EmptyRowIdentifier(), "people");
    }

    public static InsertStatement createPeopleInsert4Columns(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "fritz");
        insertValues.put("age", "14");
        insertValues.put("sex", "m");
        insertValues.put("birthday", "2024-02-02");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "people"
        );
    }

    public static InsertStatement createPeopleInsertOtherOrder(){
        Map<String, String> insertValues = new LinkedHashMap<>();
        insertValues.put("name", "tim");
        insertValues.put("birthday", "1024-02-02");
        insertValues.put("age", "14");

        return new DefaultInsertStatement(
                insertValues,
                new EmptyRowIdentifier(),
                "people"
        );
    }
}
