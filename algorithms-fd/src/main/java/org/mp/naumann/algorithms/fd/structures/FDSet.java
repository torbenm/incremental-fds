package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;

public class FDSet {

	private List<ObjectOpenHashSet<OpenBitSet>> fdLevels;
	
	private int depth = 0;
	private int maxDepth;
	
	public FDSet(int numAttributes, int maxDepth) {
		this.maxDepth = maxDepth;
		this.fdLevels = new ArrayList<>(numAttributes);
		for (int i = 0; i <= numAttributes; i++)
			this.fdLevels.add(new ObjectOpenHashSet<>());
	}

	public int getDepth() {
		return this.depth;
	}

	public int getMaxDepth() {
		return this.maxDepth;
	}

	public boolean add(OpenBitSet fd) {
		int length = (int) fd.cardinality();
		
		if (invalidLength(length))
			return false;
		
		this.depth = Math.max(this.depth, length);
		return this.fdLevels.get(length).add(fd);
	}

	public boolean contains(OpenBitSet fd) {
		int length = (int) fd.cardinality();
		return !(invalidLength(length))
				&& this.fdLevels.get(length).contains(fd);
	}
	
	public void trim(int newDepth) {
		while (this.fdLevels.size() > (newDepth + 1)) // +1 because uccLevels contains level 0
			this.fdLevels.remove(this.fdLevels.size() - 1);
		
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	private boolean invalidLength(int length){
        return (this.maxDepth > 0) && (length > this.maxDepth);
    }

}
