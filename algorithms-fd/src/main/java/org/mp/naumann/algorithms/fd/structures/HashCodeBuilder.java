package org.mp.naumann.algorithms.fd.structures;

import org.apache.commons.lang3.builder.Builder;

public class HashCodeBuilder implements Builder<Long> {

    private static final int DEFAULT_INITIAL_VALUE = 17;
    private static final int DEFAULT_MULTIPLIER_VALUE = 37;
    private final long constant;
    private long total;

    public HashCodeBuilder() {
        this(DEFAULT_INITIAL_VALUE, DEFAULT_MULTIPLIER_VALUE);
    }

    public HashCodeBuilder(int initialValue, int multiplierValue) {
        this.constant = multiplierValue;
        this.total = initialValue;
    }

    public void append(String value) {
        if (value == null)
            return;
        this.total = this.total * constant + value.hashCode();
    }

    @Override
    public Long build() {
        return total;
    }
}
