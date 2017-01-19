package org.mp.naumann.algorithms.fd.structures;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class CountingSet<T> implements Set<T> {

    private final Map<T, Integer> valueCount = new HashMap<T, Integer>();

    @Override
    public int size() {
        return valueCount.size();
    }

    @Override
    public boolean isEmpty() {
        return valueCount.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return valueCount.containsKey(o);
    }

    @Override
    public Iterator<T> iterator() {
        return valueCount.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return valueCount.keySet().toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return valueCount.keySet().toArray(a);
    }

    @Override
    public boolean add(T t) {
        if(!this.contains(t)) {
            this.valueCount.put(t, 1);
            return true;
        }else {
            this.valueCount.put(t, valueCount.get(t)+1);
            return false;
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "suspicious"})
    public boolean remove(Object o) {
        if(!valueCount.containsKey(o))
            return false;
        int count = valueCount.get(o);
        if(count > 1) {
            // not unchecked, cause we checked that the map contains this key
            valueCount.put((T)o, count-1);
            return false;
        }
        return this.valueCount.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return valueCount.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return c.parallelStream().allMatch(this::add);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return valueCount.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return c.parallelStream().allMatch(this::remove);
    }

    @Override
    public void clear() {
        valueCount.clear();
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder("[");
        for(Map.Entry<T, Integer> entry : valueCount.entrySet()){
            stringBuilder.append(entry.getKey());
            stringBuilder.append(": ");
            stringBuilder.append(entry.getValue());
            stringBuilder.append(", ");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
