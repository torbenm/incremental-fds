package org.mp.naumann.algorithms.fd.structures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class LimitedCollection<T> implements Collection<T> {

    private final Collection<T> innerCollection;
    private final int maxSize;

    public LimitedCollection(Collection<T> innerCollection, int maxSize) {
        this.innerCollection = innerCollection;
        this.maxSize = maxSize;
    }

    @Override
    public int size() {
        return innerCollection.size();
    }

    @Override
    public boolean isEmpty() {
        return innerCollection.isEmpty();
    }

    public boolean isFull() {
        return size() >= maxSize;
    }

    @Override
    public boolean contains(Object o) {
        return innerCollection.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return innerCollection.iterator();
    }

    @Override
    public Object[] toArray() {
        return innerCollection.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return innerCollection.toArray(a);
    }

    @Override
    public boolean add(T t) {
        if(!isFull())
            return innerCollection.add(t);
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return innerCollection.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return innerCollection.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        if(c.size() + this.size() > this.maxSize){
            c.forEach(this::add);
            return false;
        } else {
            return innerCollection.addAll(c);
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return innerCollection.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return innerCollection.retainAll(c);
    }

    @Override
    public void clear() {
        innerCollection.clear();
    }
}
