package org.mp.naumann.algorithms.fd.incremental.bloom;

class ColumnValue implements Comparable<ColumnValue> {

    private final String column;
    private final String value;

    public ColumnValue(String column, String value) {
        this.column = column;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(ColumnValue o) {
        return column.compareTo(o.column);
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
        return this.column.equals(other.column) && this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        final int prime = 37;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return column + "=" + value;
    }

}
