package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.violations.tree.ViolationTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TreeViolationCollection implements ViolationCollection {

    private final ViolationTreeElement rootElement;
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private int numAttributes;

    public TreeViolationCollection() {
        this.rootElement = new ViolationTreeElement(new OpenBitSet(), -1);
    }

    @Override
    public void add(OpenBitSet attr, int violatingRecord1, int violatingRecord2) {
        OpenBitSet flipped = attr.clone();
        flipped.flip(0, numAttributes);
        rootElement.add(flipped, new ViolatingPair(violatingRecord1, violatingRecord2), flipped.nextSetBit(0));
    }

    @Override
    public Collection<OpenBitSetFD> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        Collection<OpenBitSetFD> affected = new ArrayList<>();
        OpenBitSet allOnes = new OpenBitSet();
        allOnes.flip(0, numAttributes);
        this.rootElement.findAffected(removedRecords,
                allOnes, affected);
        affected.forEach(ofd -> negativeCoverToUpdate.remove(ofd.getLhs()));
        return affected;
    }

    @Override
    public void addInvalidFd(Collection<OpenBitSetFD> fd) {
        invalidFDs.addAll(fd);
    }

    @Override
    public List<OpenBitSetFD> getInvalidFds() {
        return invalidFDs;
    }
    @Override
    public boolean isInvalid(OpenBitSet lhs, int rhs) {
        return false;
    }

    @Override
    public void setNumAttributes(int numAttributes) {
        this.rootElement.setNumAttributes(numAttributes);
        this.numAttributes = numAttributes;
    }
}
