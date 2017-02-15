package org.mp.naumann.processor.exceptions;

public class BatchHandlingException extends Exception {

    public BatchHandlingException() {
    }

    public BatchHandlingException(String message) {
        super(message);
    }

    public BatchHandlingException(String message, Throwable cause) {
        super(message, cause);
    }

    public BatchHandlingException(Throwable cause) {
        super(cause);
    }

    public BatchHandlingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
