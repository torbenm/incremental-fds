package org.mp.naumann.database.jdbc.sql;

import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mp.naumann.database.jdbc.sql.helper.DeleteStatements;
import org.mp.naumann.database.statement.DeleteStatement;

public class DeleteStatementQueryBuilderTest {

    private DeleteStatementQueryBuilder dsqb = DeleteStatementQueryBuilder.get();

    @Test
    public void testGenerateQueryForDeleteStatement() throws QueryBuilderException {
        DeleteStatement statement = DeleteStatements.createDeleteStatement1();
        String expected = "DELETE FROM test.places WHERE" +
                " (country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden');";
        assertEquals(expected, dsqb.generateSingle(statement));
    }

    @Test(expected = QueryBuilderException.class)
    public void testGenerateQueryForEmptyDeleteStatement() throws QueryBuilderException {
        DeleteStatement statement = DeleteStatements.createDeleteStatementEmptyValueMap();
        SqlQueryBuilder.generateSql(statement);
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatements() throws QueryBuilderException {
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2()
        );
        String expected = "DELETE FROM test.places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')"+
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatementsButDifferentColumns() throws QueryBuilderException {
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2(),
                DeleteStatements.createDeleteStatement2Columns()
        );
        String expected = "DELETE FROM test.places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')"+
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.')"+
                " OR (country = 'US' AND city = 'San Francisco');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatementsButOtherTable() throws QueryBuilderException {
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2(),
                DeleteStatements.createDeleteStatementOtherTable(),
                DeleteStatements.createDeleteStatement2Columns()
        );
        String expected = "DELETE FROM test.places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')" +
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.')" +
                " OR (country = 'US' AND city = 'San Francisco');\n" +
                "DELETE FROM test.persons WHERE (name = 'Max' AND age = '15');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

}
