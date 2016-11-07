package org.mp.naumann.algorithms.result;

public class SimpleObjectResultSet implements ResultSet {

    private final Object value;

    public SimpleObjectResultSet(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
