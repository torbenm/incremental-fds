package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.TreeSet;

public final class ValueCombinationFunnel implements Funnel<Set<ColumnValue>> {

	private static final Charset CHARSET = Charset.defaultCharset();
	private static final long serialVersionUID = 5774622439859844362L;

	@Override
	public void funnel(Set<ColumnValue> from, PrimitiveSink into) {
		Set<ColumnValue> set = new TreeSet<>(from);
		for(ColumnValue entry : set) {
			into.putInt(entry.getColumn());
			into.putString(entry.getValue(), CHARSET);
		}
	}
}