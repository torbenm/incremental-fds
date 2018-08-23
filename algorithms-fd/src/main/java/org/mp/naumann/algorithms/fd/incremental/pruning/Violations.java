package org.mp.naumann.algorithms.fd.incremental.pruning;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntConsumer;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class Violations {

	private final Int2ObjectMap<Collection<OpenBitSetFD>> violated = new Int2ObjectOpenHashMap<>();
	private final Collection<OpenBitSetFD> tracked = new HashSet<>();
	
	public void add(IntegerPair pair, OpenBitSetFD e) {
		if (!tracked.contains(e)) {
			tracked.add(e);
			int a = pair.a();
			add(a, e);
			int b = pair.b();
			add(b, e);
		}
	}

	private void add(int recordId, OpenBitSetFD e) {
		Collection<OpenBitSetFD> violatedBy = violated.computeIfAbsent(recordId, HashSet::new);
		violatedBy.add(e);
	}

	public ValidationPruner createPruner(IntIterable deleted) {
		deleted.forEach((IntConsumer) this::pop);
		return new ViolationsPruner(tracked);
	}
	
	private void pop(int i) {
		Collection<OpenBitSetFD> violatedBy = Optional.ofNullable(violated.remove(i)).orElseGet(Collections::emptyList);
		violatedBy.forEach(tracked::remove);
//		for (OpenBitSetFD openBitSet : violatedBy) {
//			IntegerPair pair = tracked.remove(openBitSet);
//			int other = pair.a() == i? pair.b() : pair.a();
//			violated.getOrDefault(other, Collections.emptySet()).remove(openBitSet);
//		}
	}

}
