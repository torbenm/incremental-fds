package org.mp.naumann.database.statement;

import java.util.Map;

public interface UpdateStatement extends Statement {

    Map<String, String> getOldValueMap();
}
