package org.mp.naumann.database.utils;

import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.data.GenericColumn;
import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnUtils {

    public static Optional<Column<Integer>> castToInteger(Column<?> base){
        try {
            Map<RowIdentifier, Integer> values = base
                    .getValues()
                    .entrySet()
                    .parallelStream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue().toString())));
            Column<Integer> newColumn = new GenericColumn<>(base.getName(), values);
            return Optional.of(newColumn);
        } catch(NumberFormatException e){
            return Optional.empty();
        }
    }

}
