package org.mp.naumann.processor.batch.source;

import org.mp.naumann.database.statement.Statement;
public interface StreamableBatchSource extends BatchSource {


    void startStreaming();

    void endStreaming();

    boolean isStreaming();

    boolean isDoneFilling();


}
