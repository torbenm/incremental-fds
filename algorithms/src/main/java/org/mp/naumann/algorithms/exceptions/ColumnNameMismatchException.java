package org.mp.naumann.algorithms.exceptions;

public class ColumnNameMismatchException extends AlgorithmException {
    public ColumnNameMismatchException() {
    }

    public ColumnNameMismatchException(String message) {
        super(message);
    }

    public ColumnNameMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public ColumnNameMismatchException(Throwable cause) {
        super(cause);
    }

    public ColumnNameMismatchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
