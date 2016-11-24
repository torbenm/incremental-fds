package org.mp.naumann.algorithms.fd.fixtures;

import com.google.common.collect.ImmutableList;

import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.data.Row;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BridgesFixture implements AlgorithmFixture{
    protected ImmutableList<String> columnNames = ImmutableList.of("column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10", "column11", "column12", "column13");
    protected int numberOfColumns = 13;
    protected String relationName = "bridges";
    protected List<Row> table = new LinkedList<>();
    protected FunctionalDependencyResultReceiver fdResultReceiver = mock(FunctionalDependencyResultReceiver.class);


    public Table getInputGenerator() throws ConnectionException {
        JdbcDataConnector jdbcDataConnector = new JdbcDataConnector(ConnectionManager.getCsvConnection("/test"));
        return jdbcDataConnector.getTable("test", relationName);
    }

    public FunctionalDependencyResultReceiver getFunctionalDependencyResultReceiver() {
        return this.fdResultReceiver;
    }


    public void verifyFunctionalDependencyResultReceiver() {
        ColumnIdentifier expectedIdentifier1 = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expectedIdentifier2 = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expectedIdentifier3 = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expectedIdentifier4 = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
        ColumnIdentifier expectedIdentifier5 = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
        ColumnIdentifier expectedIdentifier6 = new ColumnIdentifier(this.relationName, this.columnNames.get(5));
        ColumnIdentifier expectedIdentifier7 = new ColumnIdentifier(this.relationName, this.columnNames.get(6));
        ColumnIdentifier expectedIdentifier8 = new ColumnIdentifier(this.relationName, this.columnNames.get(7));
        ColumnIdentifier expectedIdentifier9 = new ColumnIdentifier(this.relationName, this.columnNames.get(8));
        ColumnIdentifier expectedIdentifier10 = new ColumnIdentifier(this.relationName, this.columnNames.get(9));
        ColumnIdentifier expectedIdentifier11 = new ColumnIdentifier(this.relationName, this.columnNames.get(10));
        ColumnIdentifier expectedIdentifier12 = new ColumnIdentifier(this.relationName, this.columnNames.get(11));
        ColumnIdentifier expectedIdentifier13 = new ColumnIdentifier(this.relationName, this.columnNames.get(12));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier11));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier11));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6), expectedIdentifier11));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier7), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier11, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier4), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier4, expectedIdentifier6), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier4, expectedIdentifier6), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier4, expectedIdentifier6), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier4, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier4), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier4, expectedIdentifier5), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier6), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier6), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier6), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier6), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier2, expectedIdentifier4), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier13, expectedIdentifier3), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier13, expectedIdentifier4, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier2, expectedIdentifier4), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier3, expectedIdentifier8), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier12, expectedIdentifier2, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier13, expectedIdentifier2, expectedIdentifier4), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier6, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier6, expectedIdentifier9), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier6, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier2, expectedIdentifier4, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier2, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier3, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier3, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier6, expectedIdentifier7), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier3, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier4, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier3, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier7, expectedIdentifier8), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier12, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier3, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier6, expectedIdentifier8), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier6, expectedIdentifier8), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier3, expectedIdentifier6), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier3, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier3, expectedIdentifier7), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier6, expectedIdentifier7), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier6, expectedIdentifier7), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier2, expectedIdentifier4, expectedIdentifier8), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier2, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier11, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier2, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier11));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier2, expectedIdentifier4, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier2, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier10));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier3, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier3, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier13, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier12, expectedIdentifier13, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier4, expectedIdentifier8, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier11, expectedIdentifier12, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier13, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier12, expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier11, expectedIdentifier13, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier10, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier12, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier13));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier11, expectedIdentifier13, expectedIdentifier5, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier12));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier13, expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier12));

        verifyNoMoreInteractions(fdResultReceiver);
    }

}
