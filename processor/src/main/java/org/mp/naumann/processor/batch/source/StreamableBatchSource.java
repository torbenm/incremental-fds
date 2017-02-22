package org.mp.naumann.processor.batch.source;

public interface StreamableBatchSource extends BatchSource {

    void startStreaming();

    void endStreaming();

    boolean isStreaming();

    boolean isDoneFilling();


}
