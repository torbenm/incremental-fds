package org.mp.naumann.algorithms.fd.structures;

public class IntColumnValue {

    private final int value;
    private final int column;

    public IntColumnValue(int column, int value) {
        this.value = value;
        this.column = column;
    }

    public int getValue() {
        return value;
    }

    public int getColumn() {
        return column;
    }

    @Override
    public int hashCode() {
        return value * 31 + column;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;
        if(!(obj instanceof IntColumnValue))
            return false;
        return ((IntColumnValue) obj).column == this.column && ((IntColumnValue) obj).value == this.value;
    }

    @Override
    public String toString() {
        return "["+column+": "+value+"]";
    }
}
