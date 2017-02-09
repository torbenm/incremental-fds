package org.mp.naumann.algorithms.fd.incremental.violations.tree;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolatingPair;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolatingPairCollection;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.Collection;

public class ViolationTreeElement {

    private final OpenBitSet cover;
    private int numAttributes;
    private final ViolatingPairCollection violatingPairs;
    private boolean isCover = false;
    private ViolationTreeElement[] children;

    public ViolationTreeElement(OpenBitSet cover, int numAttributes) {
        this.cover = cover;
        this.numAttributes = numAttributes;
        violatingPairs = new ViolatingPairCollection();
    }

    public void add(OpenBitSet attr, ViolatingPair violatingPair, int currentLhs) {
        int nextLhs = attr.nextSetBit(currentLhs + 1);
        if(nextLhs < 0){
            // Found our guy!
            isCover = true;
            this.violatingPairs.add(violatingPair);
        }else{
            getOrAddChild(currentLhs).add(attr, violatingPair, nextLhs);
        }
    }

    private ViolationTreeElement getOrAddChild(int index){
        if(children == null)
            children = new ViolationTreeElement[numAttributes];
        if(children[index] == null){
            OpenBitSet cover = this.cover.clone();
            cover.set(index);
            children[index] = new ViolationTreeElement(cover, numAttributes);
        }
        return children[index];
    }

    public void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
    }

    public void findAffected(Collection<Integer> removedRecords, OpenBitSet available,
                    Collection<OpenBitSetFD> affected) {
        if(isCover){

            violatingPairs.removeAllIntersections(removedRecords);
            if(violatingPairs.size() == 0){
                // --> Export
                OpenBitSet rhs = available.clone();
                rhs.and(cover);
                OpenBitSet lhs = cover.clone();
                lhs.flip(0, numAttributes);
                for(int rhsAttr : BitSetUtils.iterable(rhs)){
                    affected.add(new OpenBitSetFD(lhs, rhsAttr));
                }
            }
            // Remove all possible rhs from this fd
            available.andNot(cover);
        }
        //TODO: can this case ever happen?
        if(available.cardinality() == 0 || children == null) return;
        for(int i = 0; i < numAttributes; i++){
            if(children[i] != null){
                children[i].findAffected(removedRecords, available.clone(), affected);
            }
        }
    }
}
