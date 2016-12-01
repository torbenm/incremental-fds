package org.mp.naumann.algorithms.fd.fixtures;

import com.google.common.collect.ImmutableList;

import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractAlgorithmTestFixture implements AlgorithmFixture {

    protected ImmutableList<String> columnNames;
    protected int numberOfColumns;
    protected String relationName = "R";
    protected List<Row> table = new LinkedList<>();
    protected FunctionalDependencyResultReceiver fdResultReceiver = mock(FunctionalDependencyResultReceiver.class);

    public Table getInputGenerator() throws InputReadException {
        Table inputGenerator = mock(Table.class);
        TableInput input = this.getTableInput();
        when(inputGenerator.open()).thenReturn(input);
        when(inputGenerator.numberOfColumns()).thenReturn(columnNames.size());
        when(inputGenerator.getRowCount()).thenReturn((long) table.size());
        return inputGenerator;
    }

    public FunctionalDependencyResultReceiver getFunctionalDependencyResultReceiver() {
        return this.fdResultReceiver;
    }

    public abstract TableInput getTableInput() throws InputReadException;

    public abstract void verifyFunctionalDependencyResultReceiver();
}
