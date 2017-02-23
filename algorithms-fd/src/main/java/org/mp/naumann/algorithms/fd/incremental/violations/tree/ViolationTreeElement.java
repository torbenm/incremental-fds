package org.mp.naumann.algorithms.fd.incremental.violations.tree;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.ViolatingPair;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolatingPairCollection;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.Collection;

public class ViolationTreeElement {

    private final OpenBitSet rhsCover;
    private int numAttributes;
    private final ViolatingPairCollection violatingPairs;
    private final int violationCollectionSize;
    private boolean isCover = false;
    private ViolationTreeElement[] children;

    public ViolationTreeElement(OpenBitSet rhsCover, int numAttributes, int violationCollectionSize) {
        this.rhsCover = rhsCover;
        this.numAttributes = numAttributes;
        this.violationCollectionSize = violationCollectionSize;
        violatingPairs = new ViolatingPairCollection();
    }

    public void add(OpenBitSet attr, ViolatingPair violatingPair, int currentRhs) {
        if(currentRhs < 0){
            // Found our guy!
            isCover = true;
            if(violatingPairs.size() >= violationCollectionSize) return;
            this.violatingPairs.add(violatingPair);
        }else{
            getOrAddChild(currentRhs).add(attr, violatingPair, attr.nextSetBit(currentRhs + 1));
        }
    }

    public ViolationTreeElement[] getChildren() {
        return children;
    }

    public boolean isCover() {
        return isCover;
    }

    public OpenBitSet getRhsCover() {
        return rhsCover;
    }

    private ViolationTreeElement getOrAddChild(int index){
        if(children == null)
            children = new ViolationTreeElement[numAttributes];
        if(children[index] == null){
            OpenBitSet cover = this.rhsCover.clone();
            cover.set(index);
            children[index] = new ViolationTreeElement(cover, numAttributes, violationCollectionSize);
        }
        return children[index];
    }

    public void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
    }

    public void findAffected(Collection<Integer> removedRecords,
                             OpenBitSet available,
                             Collection<OpenBitSetFD> affected) {

        checkAffectedness(removedRecords, available, affected);

        //TODO: can this first case ever happen?
        int nextPossRhs = 0;
        if(rhsCover.cardinality() > 0){
            nextPossRhs = available.nextSetBit(rhsCover.nextSetBit(0));
            if(nextPossRhs < 0) return;
        }
        if(children == null) return;

       for(int rhs = nextPossRhs; rhs >= 0; rhs = available.nextSetBit(rhs+1)){
            if(children[rhs] != null){
                children[rhs].findAffected(removedRecords, available.clone(), affected);
            }
        }
    }

    protected void checkAffectedness(Collection<Integer> removedRecords,
                                   OpenBitSet available,
                                   Collection<OpenBitSetFD> affected){
        if(isCover){
            if(isAffected(removedRecords)){
                exportPossibleFds(available, affected);
            }
            trimRhsWithCover(available);
        }
    }

    protected boolean isAffected(Collection<Integer> removedRecords){
        violatingPairs.removeAllIntersections(removedRecords);
        return violatingPairs.size() == 0;
    }

    protected void trimRhsWithCover(OpenBitSet availableRhs){
        availableRhs.andNot(rhsCover);
    }

    protected void exportPossibleFds(OpenBitSet available,Collection<OpenBitSetFD> affected){

        // --> Export
        OpenBitSet rhs = available.clone();
        rhs.and(rhsCover);

        OpenBitSet lhs = rhsCover.clone();
        lhs.flip(0, numAttributes);

        for(int rhsAttr : BitSetUtils.iterable(rhs)){
            affected.add(new OpenBitSetFD(lhs, rhsAttr));
        }
    }
}
