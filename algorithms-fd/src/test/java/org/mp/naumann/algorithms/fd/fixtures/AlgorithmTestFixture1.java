package org.mp.naumann.algorithms.fd.fixtures;

import com.google.common.collect.ImmutableList;

import org.mp.naumann.algorithms.exceptions.CouldNotReceiveResultException;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.data.GenericRow;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AlgorithmTestFixture1 extends AbstractAlgorithmTestFixture {

    public AlgorithmTestFixture1() throws CouldNotReceiveResultException {
        this.columnNames = ImmutableList.of("A", "B", "C", "D");
        this.numberOfColumns = 4;
        this.table.add(GenericRow.ofColumnNames(columnNames, "1", "1", "0", "0"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "1", "2", "1", "4"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "3", "1", "3", "0"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "2", "2", "5", "4"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "1", "1", "0", "0"));
    }

    public TableInput getTableInput() {
        TableInput input = mock(TableInput.class);

        when(input.getColumnNames())
                .thenReturn(this.columnNames);
        when(input.numberOfColumns())
                .thenReturn(this.numberOfColumns);
        when(input.getName())
                .thenReturn(this.relationName);

        when(input.hasNext())
                .thenReturn(true)
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.FALSE);

        when(input.next())
                .thenReturn(this.table.get(0))
                .thenReturn(this.table.get(1))
                .thenReturn(this.table.get(2))
                .thenReturn(this.table.get(3))
                .thenReturn(this.table.get(4));

        return input;
    }

    public void verifyFunctionalDependencyResultReceiver() {
        ColumnIdentifier expA = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expB = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expC = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expD = new ColumnIdentifier(this.relationName, this.columnNames.get(3));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expC), expB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expB), expD));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expC), expA));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expC), expD));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expD), expB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expA, expB), expC));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expA, expD), expC));

        verifyNoMoreInteractions(this.fdResultReceiver);
    }

}
