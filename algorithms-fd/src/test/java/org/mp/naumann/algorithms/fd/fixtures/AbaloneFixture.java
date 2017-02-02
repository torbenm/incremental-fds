package org.mp.naumann.algorithms.fd.fixtures;

import com.google.common.collect.ImmutableList;

import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;

import java.util.LinkedList;
import java.util.List;

import ResourceConnection.ResourceConnector;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


public class AbaloneFixture implements AlgorithmFixture{

    protected ImmutableList<String> columnNames = ImmutableList.of("column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9");
    protected int numberOfColumns = 9;
    protected int rowPosition;
    protected String relationName = "abalone";
    protected ColumnIdentifier expectedIdentifier1 = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
    protected ColumnIdentifier expectedIdentifier2 = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
    protected ColumnIdentifier expectedIdentifier3 = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
    protected ColumnIdentifier expectedIdentifier4 = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
    protected ColumnIdentifier expectedIdentifier5 = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
    protected ColumnIdentifier expectedIdentifier6 = new ColumnIdentifier(this.relationName, this.columnNames.get(5));
    protected ColumnIdentifier expectedIdentifier7 = new ColumnIdentifier(this.relationName, this.columnNames.get(6));
    protected ColumnIdentifier expectedIdentifier8 = new ColumnIdentifier(this.relationName, this.columnNames.get(7));
    protected ColumnIdentifier expectedIdentifier9 = new ColumnIdentifier(this.relationName, this.columnNames.get(8));
    protected List<ImmutableList<String>> table = new LinkedList<>();
    protected FunctionalDependencyResultReceiver fdResultReceiver = mock(FunctionalDependencyResultReceiver.class);


    public Table getInputGenerator() throws ConnectionException {
        JdbcDataConnector jdbcDataConnector = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceConnector.TEST, ","));
        Table t = jdbcDataConnector.getTable("test", relationName);
        return t;
    }

    @Override
    public FunctionalDependencyResultReceiver getFunctionalDependencyResultReceiver() {
        return this.fdResultReceiver;
    }

    @Override
    public void verifyFunctionalDependencyResultReceiver() {
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier8), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier6, expectedIdentifier7), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier2, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier1, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier5), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier8), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier8), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier8), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier5, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier3, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier7), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier5, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier6), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier5, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier2, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier6), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8, expectedIdentifier9), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier5, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier7), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier6, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier4, expectedIdentifier7, expectedIdentifier8, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier6), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier5, expectedIdentifier7), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier3, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier5));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier4, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier7, expectedIdentifier9), expectedIdentifier8));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier7), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier7));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier6, expectedIdentifier8), expectedIdentifier9));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier1));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier2));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier3));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier4));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier6));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifier5, expectedIdentifier7, expectedIdentifier8), expectedIdentifier9));

        verifyNoMoreInteractions(fdResultReceiver);
    }


}
