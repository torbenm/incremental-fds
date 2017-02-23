package org.mp.naumann.algorithms.fd.incremental.violations;

import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.ViolatingPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ViolatingPairCollection implements Collection<ViolatingPair> {

    private final Collection<ViolatingPair> violatingPairs;

    public ViolatingPairCollection(Collection<ViolatingPair> violatingPairs) {
        this.violatingPairs = violatingPairs;
    }

    public ViolatingPairCollection() {
        this.violatingPairs = new ArrayList<>();
    }

    @Override
    public int size() {
        return violatingPairs.size();
    }

    @Override
    public boolean isEmpty() {
        return violatingPairs.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return violatingPairs.contains(o);
    }

    @Override
    public Iterator<ViolatingPair> iterator() {
        return violatingPairs.iterator();
    }

    @Override
    public Object[] toArray() {
        return violatingPairs.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return violatingPairs.toArray(a);
    }

    @Override
    public boolean add(ViolatingPair violatingPair) {
        return violatingPairs.add(violatingPair);
    }

    @Override
    public boolean remove(Object o) {
        return violatingPairs.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return violatingPairs.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends ViolatingPair> c) {
        return violatingPairs.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return violatingPairs.removeAll(c);
    }

    public boolean removeAllIntersections(Collection<Integer> records){
        boolean hasRemoved = false;
        List<ViolatingPair> forRemoval = new ArrayList<>(this.size());
        for(ViolatingPair pair : this){
            for(int recordId : records){
                if(pair.intersects(recordId)){
                    forRemoval.add(pair);
                    hasRemoved = true;
                    break;
                }
            }
        }
        this.removeAll(forRemoval);
        return hasRemoved;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return violatingPairs.retainAll(c);
    }

    @Override
    public void clear() {
        violatingPairs.clear();
    }
}
