package org.mp.naumann.algorithms.result;

import java.io.PrintStream;

/**
 * Simple {@link ResultListener} that displays the result on the console
 * in the format of
 * NAME : RESULT
 *
 * @param <T> The type of the result
 */
public class PrintResultListener<T> implements ResultListener<T> {

    private final PrintStream out;
    private final String name;


    /**
     * Constructs a new PrintResultListener with the given name and standard output stream
     * System.out
     *
     * @param name The name to be displayed
     */
    public PrintResultListener(String name) {
        this(name, System.out);
    }

    /**
     * Constructs a new PrintResultListener with the given algorithm name and output stream.
     *
     * @param name The name to be displayed
     * @param out  The output stream to which the results should be printed
     */
    public PrintResultListener(String name, PrintStream out) {
        this.name = name;
        this.out = out;
    }

    @Override
    public void receiveResult(T result) {
        out.println(name + ": " + result);
    }

}
