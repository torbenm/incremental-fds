package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.fd.utils.FileUtils;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnIdentifier;

import java.io.Closeable;
import java.util.stream.Collectors;

public class TableInputProvider {

    private final Table table;
    private TableInput tableInput;

    public TableInputProvider(Table table) {
        if (table == null)
            throw new IllegalStateException("No input generator set!");
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public int getNumberOfAttributes(){
        return table.numberOfColumns();
    }

    public ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
        ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<>(table.numberOfColumns());
        columnIdentifiers.addAll(
                table.getColumnNames()
                        .stream()
                        .map(attributeName ->
                                new ColumnIdentifier(table.getName(), attributeName))
                        .collect(Collectors.toList()));
        return columnIdentifiers;
    }

    public TableInput getInput() {
        try {
            if(tableInput != null)
                tableInput = table.open();
            return tableInput;
        } catch (InputReadException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load input!", e);
        }
    }



    public void closeInput() {
        FileUtils.close(tableInput);
    }
}
