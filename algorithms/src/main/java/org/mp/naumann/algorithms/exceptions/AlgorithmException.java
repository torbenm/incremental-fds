package org.mp.naumann.algorithms.exceptions;

public class AlgorithmException extends Exception {

    private static final long serialVersionUID = 663943568188782865L;

    public AlgorithmException() {
    }

    public AlgorithmException(String message) {
        super(message);
    }

    public AlgorithmException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlgorithmException(Throwable cause) {
        super(cause);
    }

    public AlgorithmException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
