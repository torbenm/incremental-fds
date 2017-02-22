package org.mp.naumann.processor.batch.source.helper;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSourceListener;

public class ExceptionThrowingBatchSourceListener implements BatchSourceListener {
    @Override
    public void batchArrived(Batch batch) {
        throw new ExceptionThrowingBatchSourceListenerException();
    }

    public static class ExceptionThrowingBatchSourceListenerException extends RuntimeException{
		private static final long serialVersionUID = 996719513663936109L;
    }
}
