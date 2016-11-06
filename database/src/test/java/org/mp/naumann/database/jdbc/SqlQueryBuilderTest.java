package org.mp.naumann.database.jdbc;

import org.junit.Test;
import org.mp.naumann.database.statement.InsertStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mp.naumann.database.jdbc.helper.InsertStatements.*;

public class SqlQueryBuilderTest {




    @Test
    public void testGenerateQueryForInsertStatement(){

        InsertStatement insertStatement = createPeopleInsert1();
        String expected = "INSERT INTO people (name, age, birthday) VALUES ('max', '15', '2016-11-01');";
        assertEquals(SqlQueryBuilder.generateSql(insertStatement), expected);

        insertStatement = createPeopleInsertEmpty();
        expected = "INSERT INTO people () VALUES ();";
        assertEquals(SqlQueryBuilder.generateSql(insertStatement), expected);

    }

    @Test
    public void testGenerateQueryForInsertStatementsAllSameTable(){
        List<InsertStatement> statements = Arrays.asList(
                    createPeopleInsert1(),
                        createPeopleInsert2(),
                        createPeopleInsert3()
        );

        String expected = "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";
        assertEquals(SqlQueryBuilder.generateSqlForInsertStatements(statements), expected);
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffTable(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsertWrongTable(),
                createPeopleInsert2(),
                createPeopleInsert3()

        );
        String expected = "INSERT INTO persons (name, age, birthday) VALUES "+
                "('max', '15', '2016-11-01');"+
                "\n"+  "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";
        assertEquals(SqlQueryBuilder.generateSqlForInsertStatements(statements), expected);
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffColumns(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert4Columns(),
                createPeopleInsert3()

        );
        String expected = "INSERT INTO people (name, age, sex, birthday) VALUES "+
                "('fritz', '14', 'm', '2024-02-02');"+
                "\n"+  "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";
        assertEquals(SqlQueryBuilder.generateSqlForInsertStatements(statements), expected);
    }

    // TODO: THIS IS CURRENTLY NOT PASSING!!
    public void testGenerateQueryForInsertStatementsWrongOrder(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert3(),
                createPeopleInsertOtherOrder()
        );
        String expected = "INSERT INTO people (name, birthday, age) VALUES "+
                "('tim', '1024-02-02', '14');"+
                "\n"+  "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";
        assertEquals(SqlQueryBuilder.generateSqlForInsertStatements(statements), expected);
    }

    @Test
    public void testGenerateQueryForDeleteStatement(){

    }

    @Test
    public void testGenerateQueryForUpdateStatement(){

    }

}
