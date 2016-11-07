package org.mp.naumann.database.data;

import java.util.List;
import java.util.Map;

public interface ValueContainer<T, IDENTIFIER> {

    List<T> toList();
    Map<IDENTIFIER, T> getValues();
    T getValue(IDENTIFIER id);
}
