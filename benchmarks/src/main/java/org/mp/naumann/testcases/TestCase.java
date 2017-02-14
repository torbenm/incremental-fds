package org.mp.naumann.testcases;

import org.mp.naumann.database.ConnectionException;

import java.io.IOException;

public interface TestCase {

    void execute() throws ConnectionException, IOException;

    Object[] sheetValues();

}
