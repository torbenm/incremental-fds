package org.mp.naumann.algorithms.exceptions;

public class AlgorithmConfigurationException extends AlgorithmException {
    private static final long serialVersionUID = 2675421942959604497L;

    public AlgorithmConfigurationException() {
    }

    public AlgorithmConfigurationException(String message) {
        super(message);
    }

    public AlgorithmConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlgorithmConfigurationException(Throwable cause) {
        super(cause);
    }

    public AlgorithmConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
