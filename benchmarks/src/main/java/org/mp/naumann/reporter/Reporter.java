package org.mp.naumann.reporter;

import java.io.IOException;

public interface Reporter {
    void writeNewLine(Object[] objects) throws IOException;
}
