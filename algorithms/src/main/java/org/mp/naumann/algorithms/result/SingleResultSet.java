package org.mp.naumann.algorithms.result;
import java.util.Collections;
import java.util.Iterator;

public class SingleResultSet<T> implements ResultSet<T> {

    private final T value;

    public SingleResultSet(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.singleton(value).iterator();
    }
}
