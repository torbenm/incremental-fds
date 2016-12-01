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

public class AlgorithmTestFixture10 extends AbstractAlgorithmTestFixture {

    public AlgorithmTestFixture10() throws CouldNotReceiveResultException {
        this.columnNames = ImmutableList.of("PROF", "CSE", "Day", "BEGIN", "END", "ROOM", "CAP", "ID");
        this.numberOfColumns = 8;
        this.table.add(GenericRow.ofColumnNames(columnNames, "NF", "AL", "Tuesday", "09:00", "11:00", "A2", "150", "1"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "DM", "NW", "Friday", "09:00", "11:00", "A2", "150", "2"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "ML", "OS", "Monday", "09:00", "12:00", "I10", "30", "3"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "NN", "PL", "Monday", "14:00", "17:00", "I10", "30", "4"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "AH", "DB", "Monday", "09:00", "12:00", "I11", "30", "3"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "RC", "SI", "Tuesday", "09:00", "12:00", "I10", "30", "5"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "KL", "OR", "Tuesday", "09:00", "12:00", "I12", "30", "5"));

        // TODO remove debugging
//		doAnswer(new Answer() {
//			public Object answer(InvocationOnMock invocation) {
//				Object[] args = invocation.getArguments();
//				System.out.println(args[0]);
//				System.out.println(args[1]);
//				return null;
//			}
//		}).when(functionalDependencyResultReceiver).receiveResult(isA(ColumnCombination.class), isA(ColumnIdentifier.class));
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
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.FALSE);

        when(input.next())
                .thenReturn(this.table.get(0))
                .thenReturn(this.table.get(1))
                .thenReturn(this.table.get(2))
                .thenReturn(this.table.get(3))
                .thenReturn(this.table.get(4))
                .thenReturn(this.table.get(5))
                .thenReturn(this.table.get(6));

        return input;
    }

    public void verifyFunctionalDependencyResultReceiver() {
        ColumnIdentifier expPROF = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expCSE = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expDAY = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expBEGIN = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
        ColumnIdentifier expEND = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
        ColumnIdentifier expROOM = new ColumnIdentifier(this.relationName, this.columnNames.get(5));
        ColumnIdentifier expCAP = new ColumnIdentifier(this.relationName, this.columnNames.get(6));
        ColumnIdentifier expID = new ColumnIdentifier(this.relationName, this.columnNames.get(7));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expCSE));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expDAY));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expBEGIN));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expEND));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expROOM));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expCAP));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expPROF), expID));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expPROF));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expDAY));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expBEGIN));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expEND));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expROOM));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expCAP));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expCSE), expID));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID), expCAP));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID), expDAY));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID), expEND));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID), expBEGIN));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expEND), expBEGIN));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expEND), expCAP));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expROOM), expCAP));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expBEGIN, expROOM), expEND));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expBEGIN, expCAP), expEND));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID, expROOM), expPROF));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expID, expROOM), expCSE));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expEND), expID));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expBEGIN, expROOM), expCSE));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expBEGIN, expROOM), expPROF));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expBEGIN, expROOM), expID));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expEND, expROOM), expCSE));
        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expEND, expROOM), expPROF));

        verify(fdResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expDAY, expBEGIN, expCAP), expID));

        verifyNoMoreInteractions(fdResultReceiver);
    }

}
