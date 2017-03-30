package org.mp.naumann.algorithms.fd.hyfd;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;

public class FDList {

    private List<List<OpenBitSet>> fdLevels;

    private int depth = 0;
    private int maxDepth;

    public FDList(int numAttributes, int maxDepth) {
        this.maxDepth = maxDepth;
        this.fdLevels = new ArrayList<>(numAttributes);
        for (int i = 0; i <= numAttributes; i++)
            this.fdLevels.add(new ArrayList<>());
    }

    public List<List<OpenBitSet>> getFdLevels() {
        return this.fdLevels;
    }

    public boolean add(OpenBitSet fd) {
        int length = (int) fd.cardinality();

        if ((this.maxDepth > 0) && (length > this.maxDepth))
            return false;

        this.depth = Math.max(this.depth, length);
        return this.fdLevels.get(length).add(fd);
    }

    public void trim(int newDepth) {
        while (this.fdLevels.size() > (newDepth + 1)) // +1 because uccLevels contains level 0
            this.fdLevels.remove(this.fdLevels.size() - 1);

        this.depth = newDepth;
        this.maxDepth = newDepth;
    }

    public int size() {
        int size = 0;
        for (List<OpenBitSet> uccs : this.fdLevels)
            size += uccs.size();
        return size;
    }
}
