package org.mp.naumann.algorithms.fd.utils;

public class ValueComparator {

    private boolean isNullEqualNull;

    public ValueComparator(boolean isNullEqualNull) {
        this.isNullEqualNull = isNullEqualNull;
    }

    public boolean isNullEqualNull() {
        return this.isNullEqualNull;
    }

    public boolean isEqual(int val1, int val2) {
        return (val1 >= 0) && (val2 >= 0) && (val1 == val2);
    }

}
