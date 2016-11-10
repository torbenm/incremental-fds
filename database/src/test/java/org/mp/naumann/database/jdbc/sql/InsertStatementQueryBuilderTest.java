package org.mp.naumann.database.jdbc.sql;

import org.junit.Test;
import org.mp.naumann.database.statement.InsertStatement;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mp.naumann.database.jdbc.sql.helper.InsertStatements.*;

public class InsertStatementQueryBuilderTest {


    private InsertStatementQueryBuilder isqb = new InsertStatementQueryBuilder();

    @Test
    public void testGenerateSingle(){
        InsertStatement insertStatement = createPeopleInsert1();
        String expected = "INSERT INTO people (name, birthday, age) VALUES ('max', '2016-11-01', '15');";
        assertEquals(isqb.generateSingle(insertStatement), expected);

        insertStatement = createPeopleInsertEmpty();
        expected = "INSERT INTO people () VALUES ();";
        assertEquals(isqb.generateSingle(insertStatement), expected);
    }

    @Test
    public void testGenerateKeyMap(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("(name, birthday, age) VALUES ", isqb.buildKeyMap(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("() VALUES ", isqb.buildKeyMap(insertStatement));
    }

    @Test
    public void testGenerateValueMap(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("('max', '2016-11-01', '15')", isqb.buildValueMap(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("()", isqb.buildValueMap(insertStatement));
    }

    @Test
    public void testOpenStatement(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("INSERT INTO people ", isqb.openStatement(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("INSERT INTO people ", isqb.openStatement(insertStatement));
    }



    @Test
    public void testGenerateQueryForInsertStatementsAllSameTable(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert3()
        );

        String expected = "INSERT INTO people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');";
        assertEquals(isqb.generateMulti(statements), expected);
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffTable(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsertWrongTable(),
                createPeopleInsert2(),
                createPeopleInsert3()

        );
        String expected = "INSERT INTO persons (name, birthday, age) VALUES "+
                "('max', '2016-11-01', '15');"+
                "\n"+  "INSERT INTO people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');";
        assertEquals(isqb.generateMulti(statements), expected);
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffColumns(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert4Columns(),
                createPeopleInsert3()

        );
        String expected = "INSERT INTO people (name, birthday, age, sex) VALUES "+
                "('fritz', '2024-02-02', '14', 'm');"+
                "\n"+   "INSERT INTO people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');";
        assertEquals(isqb.generateMulti(statements), expected);
    }

    @Test
    public void testGenerateQueryForInsertStatementsWrongOrder(){
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert3(),
                createPeopleInsertOtherOrder()
        );
        // One of these has to be passed.
        // TODO: How to check both and accept if one is correct?
        String expected = "INSERT INTO people (name, birthday, age) VALUES "+
                "('tim', '1024-02-02', '14');"+
                "\n"+  "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";

        String expected2 = "INSERT INTO people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029'), " +
                "('tim', '1024-02-02', '14');";

        assertEquals(isqb.generateMulti(statements), expected2);
    }
}
