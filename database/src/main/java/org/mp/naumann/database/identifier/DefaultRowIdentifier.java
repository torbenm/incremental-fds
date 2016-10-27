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
        if ((object instanceof RowIdentifier) && (((RowIdentifier)object).getRowId() == rowId)) {
            return true;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return ((Integer)rowId).hashCode();
    }
}
