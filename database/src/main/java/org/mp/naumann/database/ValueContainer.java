package org.mp.naumann.database;

import java.util.List;

public interface ValueContainer<T, IDENTIFIER> {

    List<T> toList();
    T getValue(IDENTIFIER id);
}
