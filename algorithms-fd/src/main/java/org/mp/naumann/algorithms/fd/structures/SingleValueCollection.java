package org.mp.naumann.algorithms.fd.structures;

import java.util.Collection;
import java.util.Iterator;

public class SingleValueCollection<E> implements Collection<E> {

    private E element;
    private boolean keepFirst = true;

    public SingleValueCollection() {
    }

    public SingleValueCollection(boolean keepFirst) {
        this.keepFirst = keepFirst;
    }

    @Override
    public int size() {
        return isEmpty() ? 0 : 1;
    }

    @Override
    public boolean isEmpty() {
        return element == null;
    }

    @Override
    public boolean contains(Object o) {
        return o.equals(element);
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private boolean notQueriedYet = true;

            @Override
            public boolean hasNext() {
                return notQueriedYet;
            }

            @Override
            public E next() {
                notQueriedYet = false;
                return element;
            }

            @Override
            public void remove() {
                clear();
            }
        };
    }

    @Override
    public Object[] toArray() {
        return new Object[]{element};
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    @Override
    public boolean add(E e) {
        if (!keepFirst || isEmpty()) {
            this.element = e;
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        if (this.contains(o)) {
            clear();
            return true;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.size() > 1) {
            return false;
        }
        return c.stream().filter(this::contains).count() == 1;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        c.forEach(this::add);
        return c.size() <= 1;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        c.forEach(this::remove);
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        if (c.stream().filter(this::contains).count() == c.size()) {
            clear();
            return true;
        }
        return false;
    }

    @Override
    public void clear() {
        this.element = null;
    }
}
