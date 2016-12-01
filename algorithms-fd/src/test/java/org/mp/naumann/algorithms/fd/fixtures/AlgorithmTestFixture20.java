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

public class AlgorithmTestFixture20 extends AbstractAlgorithmTestFixture {

    public AlgorithmTestFixture20() throws CouldNotReceiveResultException {
        this.columnNames = ImmutableList.of("A", "B", "C", "D", "E", "F");
        this.numberOfColumns = this.columnNames.size();
        this.table.add(GenericRow.ofColumnNames(columnNames, "a", "a", "b", "c", "a", "c"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "b", "c", "a", "d", "a", "c"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "c", "a", "c", "b", "c", "d"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "d", "d", "c", "d", "b", "d"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "e", "d", "c", "d", "e", "d"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "f", "d", "c", "d", "b", "f"));
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
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.TRUE)
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
                .thenReturn(this.table.get(4))
                .thenReturn(this.table.get(5));

        return input;
    }

    public void verifyFunctionalDependencyResultReceiver() {
        ColumnIdentifier expectedIdentifierA = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expectedIdentifierB = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expectedIdentifierC = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expectedIdentifierD = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
        ColumnIdentifier expectedIdentifierE = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
        ColumnIdentifier expectedIdentifierF = new ColumnIdentifier(this.relationName, this.columnNames.get(5));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierA), expectedIdentifierB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierA), expectedIdentifierC));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierA), expectedIdentifierD));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierA), expectedIdentifierE));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierA), expectedIdentifierF));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierC), expectedIdentifierD));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierD), expectedIdentifierC));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierE), expectedIdentifierC));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierE), expectedIdentifierD));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierE, expectedIdentifierF), expectedIdentifierA));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierF), expectedIdentifierC));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierB, expectedIdentifierF), expectedIdentifierD));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierC, expectedIdentifierD), expectedIdentifierB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierC, expectedIdentifierE), expectedIdentifierB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierC, expectedIdentifierE), expectedIdentifierD));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierC, expectedIdentifierE, expectedIdentifierF), expectedIdentifierA));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierD, expectedIdentifierE), expectedIdentifierB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierD, expectedIdentifierE), expectedIdentifierC));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierD, expectedIdentifierE, expectedIdentifierF), expectedIdentifierA));

        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierD, expectedIdentifierF), expectedIdentifierB));
        verify(this.fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierD, expectedIdentifierF), expectedIdentifierC));

        verifyNoMoreInteractions(this.fdResultReceiver);
    }
}
