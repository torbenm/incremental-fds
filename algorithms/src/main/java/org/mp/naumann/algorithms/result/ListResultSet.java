package org.mp.naumann.algorithms.result;

import java.util.Iterator;
import java.util.List;

public class ListResultSet<T> implements ResultSet<T> {

    private final List<T> resultList;

    public ListResultSet(List<T> resultList) {
        this.resultList = resultList;
    }

    @Override
    public Iterator<T> iterator() {
        return resultList.iterator();
    }
}
