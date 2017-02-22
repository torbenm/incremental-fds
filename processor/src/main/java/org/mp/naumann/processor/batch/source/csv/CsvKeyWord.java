package org.mp.naumann.processor.batch.source.csv;

import java.util.Arrays;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum CsvKeyWord {
    ACTION_COLUMN("::action"),
    INSERT_STATEMENT("insert"),
    DELETE_STATEMENT("delete"),
    UPDATE_STATEMENT("update");

    private final String keyWord;

    public static CsvKeyWord valueOfKeyWord(String value){
        return Arrays.stream(values())
                .filter(k -> k.getKeyWord().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }
}
