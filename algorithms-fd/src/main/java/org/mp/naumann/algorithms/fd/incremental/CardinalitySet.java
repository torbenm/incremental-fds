package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;

class CardinalitySet {

	private List<ObjectOpenHashSet<OpenBitSet>> levels;
	
	private int depth = 0;
	private int maxDepth;
	
	public CardinalitySet(int maxDepth) {
		this.maxDepth = maxDepth;
		this.levels = new ArrayList<>(maxDepth);
		for (int i = 0; i <= maxDepth; i++)
			this.levels.add(new ObjectOpenHashSet<>());
	}

	public int getDepth() {
		return this.depth;
	}

	public int getMaxDepth() {
		return this.maxDepth;
	}

	public boolean add(OpenBitSet fd) {
		int length = (int) fd.cardinality();
		
		if ((this.maxDepth > 0) && (length > this.maxDepth))
			return false;
		
		this.depth = Math.max(this.depth, length);
		return this.levels.get(length).add(fd);
	}

	public boolean contains(OpenBitSet fd) {
		int length = (int) fd.cardinality();
		
		if ((this.maxDepth > 0) && (length > this.maxDepth))
			return false;
		
		return this.levels.get(length).contains(fd);
	}
	
	public void trim(int newDepth) {
		while (this.levels.size() > (newDepth + 1)) // +1 because uccLevels contains level 0
			this.levels.remove(this.levels.size() - 1);
		
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	public ObjectOpenHashSet<OpenBitSet> getLevel(int level) {
		return levels.get(level);
	}

}
