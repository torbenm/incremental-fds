package org.mp.naumann.database.identifier;

public class DefaultRowIdentifier implements RowIdentifier {
    private final int rowId;

    public DefaultRowIdentifier(int rowId) {
        this.rowId = rowId;
    }

    public int getRowId() {
        return rowId;
    }

    public boolean equals(Object object) {
        return ((object instanceof RowIdentifier) && (((RowIdentifier)object).getRowId() == rowId));
    }

    public int hashCode() {
        return ((Integer)rowId).hashCode();
    }
}
