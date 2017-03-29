package org.mp.naumann.database;

public class InputReadException extends Exception {

    private static final long serialVersionUID = 2013310216230615556L;

    public InputReadException() {

    }

    public InputReadException(String message) {
        super(message);
    }

    public InputReadException(Throwable cause) {
        super(cause);
    }

    public InputReadException(String message, Throwable cause) {
        super(message, cause);
    }

}
