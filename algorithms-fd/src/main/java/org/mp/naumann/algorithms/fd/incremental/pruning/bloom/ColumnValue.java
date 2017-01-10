package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

class ColumnValue implements Comparable<ColumnValue> {

    private final int column;
    private final String value;

    public ColumnValue(int column, String value) {
        this.column = column;
        this.value = value;
    }

    public int getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(ColumnValue o) {
        return Integer.compare(column, o.column);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        ColumnValue other = (ColumnValue) obj;
        return this.column == other.column && this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        final int prime = 37;
        int result = 1;
        result = prime * result + Integer.hashCode(column);
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return column + "=" + value;
    }

}
