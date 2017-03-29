package org.mp.naumann.algorithms.exceptions;


public class AlgorithmExecutionException extends AlgorithmException {

    private static final long serialVersionUID = -7520456690085803919L;

    public AlgorithmExecutionException() {
    }

    public AlgorithmExecutionException(String message) {
        super(message);
    }

    public AlgorithmExecutionException(Throwable cause) {
        super(cause);
    }

    public AlgorithmExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}

