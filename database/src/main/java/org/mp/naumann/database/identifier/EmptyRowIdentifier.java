package org.mp.naumann.database.identifier;

public class EmptyRowIdentifier implements RowIdentifier {


    @Override
    public int getRowId() {
        return 0;
    }
}
