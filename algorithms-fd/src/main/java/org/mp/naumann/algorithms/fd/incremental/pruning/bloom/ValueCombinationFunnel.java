package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.nio.charset.Charset;
import java.util.Collection;

public final class ValueCombinationFunnel implements Funnel<Collection<ColumnValue>> {

    private static final Charset CHARSET = Charset.defaultCharset();
    private static final long serialVersionUID = 5774622439859844362L;

    @Override
    public void funnel(Collection<ColumnValue> from, PrimitiveSink into) {
        for (ColumnValue entry : from) {
            into.putInt(entry.getColumn());
            into.putString(entry.getValue(), CHARSET);
        }
    }
}