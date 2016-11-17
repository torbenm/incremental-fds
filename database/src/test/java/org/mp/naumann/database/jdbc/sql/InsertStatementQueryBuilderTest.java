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
    public void testGenerateSingle() throws QueryBuilderException {
        InsertStatement insertStatement = createPeopleInsert1();
        String expected = "INSERT INTO test.people (name, birthday, age) VALUES ('max', '2016-11-01', '15');";
        assertEquals(isqb.generateSingle(insertStatement), expected);
    }

    @Test(expected = QueryBuilderException.class)
    public void testEmptyStatement() throws QueryBuilderException {
        InsertStatement insertStatement = createPeopleInsertEmpty();
        isqb.generateSingle(insertStatement);
    }

    @Test
    public void testGenerateKeyMap(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("(name, birthday, age) VALUES ", isqb.buildKeyClause(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("() VALUES ", isqb.buildKeyClause(insertStatement));
    }

    @Test
    public void testGenerateValueMap(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("('max', '2016-11-01', '15')", isqb.buildValueClause(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("()", isqb.buildValueClause(insertStatement));
    }

    @Test
    public void testOpenStatement(){
        InsertStatement insertStatement = createPeopleInsert1();
        assertEquals("INSERT INTO test.people ", isqb.openStatement(insertStatement));

        insertStatement = createPeopleInsertEmpty();
        assertEquals("INSERT INTO test.people ", isqb.openStatement(insertStatement));
    }

    @Test
    public void testGenerateQueryForInsertStatementsAllSameTable() throws QueryBuilderException {
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert3()
        );

        String expected = "INSERT INTO test.people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');";
        assertEquals(expected, isqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffTable() throws QueryBuilderException {
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsertWrongTable(),
                createPeopleInsert2(),
                createPeopleInsert3()

        );
        String expected = "INSERT INTO test.people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');" +
                "\n" + "INSERT INTO test.persons (name, birthday, age) VALUES "+
                "('max', '2016-11-01', '15');";
        assertEquals(expected, isqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForInsertStatementsDiffColumns() throws QueryBuilderException {
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert4Columns(),
                createPeopleInsert3(),
                createPeopleInsert4Columns2()
        );
        String expected = "INSERT INTO test.people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029');" +
                "\n" + "INSERT INTO test.people (name, birthday, age, sex) VALUES " +
                "('fritz', '2024-02-02', '14', 'm'), " +
                "('hanna', '2014-12-03', '29', 'f');";
        assertEquals(expected, isqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForInsertStatementsWrongOrder() throws QueryBuilderException {
        List<InsertStatement> statements = Arrays.asList(
                createPeopleInsert1(),
                createPeopleInsert2(),
                createPeopleInsert3(),
                createPeopleInsertOtherOrder()
        );
        // One of these has to be passed.
        // TODO: How to check both and accept if one is correct?
        String expected = "INSERT INTO test.people (name, birthday, age) VALUES "+
                "('tim', '1024-02-02', '14');"+
                "\n"+  "INSERT INTO people (name, age, birthday) VALUES " +
                "('max', '15', '2016-11-01'), " +
                "('hanna', '29', '2014-12-03'), " +
                "('frieda', '1029', '1024-02-02');";

        String expected2 = "INSERT INTO test.people (name, birthday, age) VALUES " +
                "('max', '2016-11-01', '15'), " +
                "('hanna', '2014-12-03', '29'), " +
                "('frieda', '1024-02-02', '1029'), " +
                "('tim', '1024-02-02', '14');";

        assertEquals(expected2, isqb.generateMulti(statements));
    }

    @Test
    public void testSpacesInNames() throws QueryBuilderException {
        InsertStatement insertStatement = createInsertSpacesInNames();
        String expected = "INSERT INTO \"test schema\".\"all people\" (name) VALUES ('tim');";
        assertEquals(expected, isqb.generateSingle(insertStatement));
    }

    @Test
    public void testQuoteInValues() throws QueryBuilderException {
        InsertStatement insertStatement = createInsertQuoteInValue();
        String expected = "INSERT INTO test.people (name) VALUES ('Max O''Connor');";
        assertEquals(expected, isqb.generateSingle(insertStatement));
    }
}
