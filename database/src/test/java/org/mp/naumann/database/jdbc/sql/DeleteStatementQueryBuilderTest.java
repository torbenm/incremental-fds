package org.mp.naumann.database.jdbc.sql;

import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mp.naumann.database.jdbc.sql.helper.DeleteStatements;
import org.mp.naumann.database.statement.DeleteStatement;

public class DeleteStatementQueryBuilderTest {

    DeleteStatementQueryBuilder dsqb = DeleteStatementQueryBuilder.get();
    @Test
    public void testGenerateQueryForDeleteStatement(){
        DeleteStatement statement = DeleteStatements.createDeleteStatement1();
        String expected = "DELETE FROM places WHERE" +
                " country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden';";
        assertEquals(expected, dsqb.generateSingle(statement));


        //TODO: Should we rather throw an error here?
        statement = DeleteStatements.createDeleteStatementEmptyValueMap();
        expected = "DELETE FROM places;";
        assertEquals(expected, SqlQueryBuilder.generateSql(statement));
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatements(){
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2()
        );
        String expected = "DELETE FROM places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')"+
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatementsButDifferentColumns(){
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2(),
                DeleteStatements.createDeleteStatement2Columns()
        );
        String expected = "DELETE FROM places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')"+
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.')"+
                " OR (country = 'US' AND city = 'San Francisco');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

    @Test
    public void testGenerateQueryForDeleteStatementMultipleStatementsButOtherTable(){
        List<DeleteStatement> statements = Arrays.asList(
                DeleteStatements.createDeleteStatement1(),
                DeleteStatements.createDeleteStatement2(),
                DeleteStatements.createDeleteStatementOtherTable(),
                DeleteStatements.createDeleteStatement2Columns()
        );
        String expected = "DELETE FROM persons WHERE name = 'Max' AND age = '15';\n"+
                "DELETE FROM places WHERE " +
                "(country = 'DE' AND city = 'Berlin' AND street = 'Unter den Linden')"+
                " OR (country = 'DE' AND city = 'Potsdam' AND street = 'August-Bebel-Str.')"+
                " OR (country = 'US' AND city = 'San Francisco');";

        assertEquals(expected, dsqb.generateMulti(statements));
    }

}
