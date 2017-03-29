package org.mp.naumann.database;

public class ConnectionException extends Exception {

    private static final long serialVersionUID = -2848384920829722229L;

    public ConnectionException() {

    }

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(Throwable cause) {
        super(cause);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
