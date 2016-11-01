package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.HasRowIdentifier;

import java.util.Map;

public interface Statement extends HasRowIdentifier {

    String getTableName();
    Map<String, String> getValueMap();
}
