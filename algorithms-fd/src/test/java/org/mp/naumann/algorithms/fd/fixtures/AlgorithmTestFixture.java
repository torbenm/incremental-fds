package org.mp.naumann.algorithms.fd.fixtures;

import com.google.common.collect.ImmutableList;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mp.naumann.algorithms.exceptions.AlgorithmConfigurationException;
import org.mp.naumann.algorithms.exceptions.ColumnNameMismatchException;
import org.mp.naumann.algorithms.exceptions.CouldNotReceiveResultException;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.data.GenericRow;
import org.mp.naumann.database.data.Row;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlgorithmTestFixture implements AlgorithmFixture {

    protected ImmutableList<String> columnNames = ImmutableList.of("PROF", "CSE", "DAY", "BEGIN", "END", "ROOM", "CAP", "ID");
    protected int numberOfColumns = 8;
    protected int rowPosition;
    protected String relationName = "testTable";
    protected List<Row> table = new LinkedList<>();
    protected FunctionalDependencyResultReceiver functionalDependencyResultReceiver = mock(FunctionalDependencyResultReceiver.class);

    public AlgorithmTestFixture() throws CouldNotReceiveResultException {
        this.table.add(GenericRow.ofColumnNames(columnNames, "NF", "AL", "Tuesday", "09:00", "09:00", "A2", "150", "Monday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "DM", "NW", "Friday", "09:00", "09:00", "A2", "150", "Tuesday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "ML", "OS", "Monday", "09:00", "14:00", "I10", "30", "Wednesday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "NN", "PL", "Monday", "14:00", "17:00", "I10", "30", "Thursday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "AH", "DB", "Monday", "09:00", "14:00", "I11", "30", "Wednesday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "RC", "SI", "Tuesday", "09:00", "14:00", "I10", "30", "Friday"));
        this.table.add(GenericRow.ofColumnNames(columnNames, "KL", "OR", "Tuesday", "09:00", "14:00", "I12", "30", "Friday"));

        this.rowPosition = 0;

    }

    /* Currently never used ??
        public static Map<ColumnCombinationBitset, PositionListIndex> getPlis(TableInput input){
            Map<ColumnCombinationBitset, PositionListIndex> plis = new HashMap<>();
            PliBuilder builder = new PliBuilder();
            List<PositionListIndex> pliList = builder.getPlis(input, input.getColumns().size(), true);
            int i = 0;
            for (PositionListIndex pli : pliList) {
                plis.put(new ColumnCombinationBitset(i++), pli);
            }
            return plis;
        }
     */
    public Table getInputGenerator() throws InputReadException {
        Table inputGenerator = mock(Table.class);
        TableInput input = this.getTableInput();
        when(inputGenerator.open()).thenReturn(input);
        return inputGenerator;
    }

    protected TableInput getTableInput() throws InputReadException {
        TableInput input = mock(TableInput.class);

        when(input.getColumnNames())
                .thenReturn(this.columnNames);
        when(input.numberOfColumns())
                .thenReturn(this.numberOfColumns);
        when(input.getName())
                .thenReturn(this.relationName);

        when(input.hasNext()).thenAnswer(new Answer<Boolean>() {
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return rowPosition < table.size();
            }
        });

        when(input.next()).thenAnswer(new Answer<Row>() {
            public Row answer(InvocationOnMock invocation) throws Throwable {
                rowPosition += 1;
                return table.get(rowPosition - 1);
            }
        });

        return input;
    }

    public FunctionalDependencyResultReceiver getFunctionalDependencyResultReceiver() {
        return this.functionalDependencyResultReceiver;
    }


    public void verifyFunctionalDependencyResultReceiver() {
        ColumnIdentifier expectedIdentifierPROF = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expectedIdentifierCSE = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expectedIdentifierDAY = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expectedIdentifierBEGIN = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
        ColumnIdentifier expectedIdentifierEND = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
        ColumnIdentifier expectedIdentifierROOM = new ColumnIdentifier(this.relationName, this.columnNames.get(5));
        ColumnIdentifier expectedIdentifierCAP = new ColumnIdentifier(this.relationName, this.columnNames.get(6));
        ColumnIdentifier expectedIdentifierID = new ColumnIdentifier(this.relationName, this.columnNames.get(7));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierROOM));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierID));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierROOM));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierID));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierBEGIN));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierEND), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierEND), expectedIdentifierCAP));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierROOM), expectedIdentifierCAP));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierEND));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierBEGIN, expectedIdentifierCAP), expectedIdentifierEND));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID, expectedIdentifierROOM), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID, expectedIdentifierROOM), expectedIdentifierCSE));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND), expectedIdentifierID));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierID));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND, expectedIdentifierROOM), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND, expectedIdentifierROOM), expectedIdentifierPROF));

        verify(functionalDependencyResultReceiver).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierCAP), expectedIdentifierID));

        //verifyNoMoreInteractions(fdResultReceiver);
    }


    public void verifyFunctionalDependencyResultReceiverForFDMine() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        ColumnIdentifier expectedIdentifierPROF = new ColumnIdentifier(this.relationName, this.columnNames.get(0));
        ColumnIdentifier expectedIdentifierCSE = new ColumnIdentifier(this.relationName, this.columnNames.get(1));
        ColumnIdentifier expectedIdentifierDAY = new ColumnIdentifier(this.relationName, this.columnNames.get(2));
        ColumnIdentifier expectedIdentifierBEGIN = new ColumnIdentifier(this.relationName, this.columnNames.get(3));
        ColumnIdentifier expectedIdentifierEND = new ColumnIdentifier(this.relationName, this.columnNames.get(4));
        ColumnIdentifier expectedIdentifierROOM = new ColumnIdentifier(this.relationName, this.columnNames.get(5));
        ColumnIdentifier expectedIdentifierCAP = new ColumnIdentifier(this.relationName, this.columnNames.get(6));
        ColumnIdentifier expectedIdentifierID = new ColumnIdentifier(this.relationName, this.columnNames.get(7));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierROOM));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierPROF), expectedIdentifierID));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierROOM));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierCSE), expectedIdentifierID));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierCAP));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierDAY));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierEND));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID), expectedIdentifierBEGIN));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierEND), expectedIdentifierBEGIN));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierEND), expectedIdentifierCAP));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierROOM), expectedIdentifierCAP));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierEND));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierBEGIN, expectedIdentifierCAP), expectedIdentifierEND));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID, expectedIdentifierROOM), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierID, expectedIdentifierROOM), expectedIdentifierCSE));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND), expectedIdentifierID));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierPROF));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierROOM), expectedIdentifierID));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND, expectedIdentifierROOM), expectedIdentifierCSE));
        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierEND, expectedIdentifierROOM), expectedIdentifierPROF));

        verify(functionalDependencyResultReceiver, atLeastOnce()).receiveResult(new FunctionalDependency(new ColumnCombination(expectedIdentifierDAY, expectedIdentifierBEGIN, expectedIdentifierCAP), expectedIdentifierID));
    }
}
