package org.mp.naumann.algorithms.implementations;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.data.Column;

public class MedianInitialAlgorithm extends InitialAlgorithm {

    private String column;
    private String table;

    public MedianInitialAlgorithm(DataConnector dataConnector, String column, String table) {
        super(dataConnector);
        this.column = column;
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public AlgorithmResult execute() {

        Column col = getDataConnector().getTable(table).getColumn(column);
        if(Number.class.isAssignableFrom(col.getColumnType())){
            System.out.println("ok!");
        }
        return null;
    }
}
