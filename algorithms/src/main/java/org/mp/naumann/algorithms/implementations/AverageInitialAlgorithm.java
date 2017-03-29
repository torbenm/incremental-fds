package org.mp.naumann.algorithms.implementations;

import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

/**
 * An implementation of an {@link InitialAlgorithm} that calculates the average over
 * a given column in a table.
 */
public class AverageInitialAlgorithm implements InitialAlgorithm<Double, AverageDatastructure> {

    private final String column;
    private final String table;
    private final DataConnector dataConnector;
    private final String schema;
    private final AverageDatastructure ds = new AverageDatastructure();

    public AverageInitialAlgorithm(String column, String table, DataConnector dataConnector, String schema) {
        this.column = column;
        this.table = table;
        this.dataConnector = dataConnector;
        this.schema = schema;
    }

    @Override
    public AverageDatastructure getIntermediateDataStructure() {
        return ds;
    }

    @Override
    public Double execute() {
        Table t = dataConnector.getTable(schema, table);
        try (TableInput input = t.open()) {
            while (input.hasNext()) {
                Row row = input.next();
                String value = row.getValue(column);
                if (value != null && !value.isEmpty()) {
                    ds.increaseSum(Double.parseDouble(value));
                }
                ds.increaseCount();
            }
            return ds.getAverage();
        } catch (InputReadException e) {
            throw new RuntimeException(e);
        }
    }

}
