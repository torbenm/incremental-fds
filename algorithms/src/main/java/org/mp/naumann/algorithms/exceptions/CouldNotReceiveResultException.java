package org.mp.naumann.algorithms.exceptions;

public class CouldNotReceiveResultException extends AlgorithmException {
    public CouldNotReceiveResultException() {
    }

    public CouldNotReceiveResultException(String message) {
        super(message);
    }

    public CouldNotReceiveResultException(String message, Throwable cause) {
        super(message, cause);
    }

    public CouldNotReceiveResultException(Throwable cause) {
        super(cause);
    }

    public CouldNotReceiveResultException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
