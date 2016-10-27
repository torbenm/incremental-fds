package org.mp.naumann.database.identifier;

public class DefaultRowIdentifier implements RowIdentifier {
    private final int rowId;

    public DefaultRowIdentifier(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public RowIdentifier getRowIdentifier() {
        return null;
    }
}
