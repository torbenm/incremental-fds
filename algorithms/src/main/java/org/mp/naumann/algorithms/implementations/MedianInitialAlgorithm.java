package org.mp.naumann.algorithms.implementations;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.SimpleObjectResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.data.Column;

import java.util.Collections;
import java.util.List;

public class MedianInitialAlgorithm extends InitialAlgorithm {

    private String column;
    private String table;

    public MedianInitialAlgorithm(DataConnector dataConnector, String table, String column) {
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
        if(Comparable.class.isAssignableFrom(col.getColumnType())){
            return executeAlgorithm(col);
        }
        return null;
    }

    protected AlgorithmResult executeAlgorithm(Column<Comparable> column){
        List<Comparable> values = column.toList();
        Collections.sort(values);
        AlgorithmResult result = new AlgorithmResult();
        result.setResultSet(new SimpleObjectResultSet(values.get(values.size()/2)));
        return result;
    }
}
