package org.mp.naumann.algorithms.fd.structures;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.TreeSet;

import org.mp.naumann.algorithms.fd.structures.ValueCombination.ColumnValue;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public final class ValueCombinationFunnel implements Funnel<Set<ColumnValue>> {

	private static final Charset CHARSET = Charset.defaultCharset();
	private static final long serialVersionUID = 5774622439859844362L;

	@Override
	public void funnel(Set<ColumnValue> from, PrimitiveSink into) {
		Set<ColumnValue> set = new TreeSet<>(from);
		for(ColumnValue entry : set) {
			into.putString(entry.getColumn(), CHARSET);
			into.putString(entry.getValue(), CHARSET);
		}
	}
}