package org.mp.naumann.algorithms.fd.incremental.structures;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class Lattice extends LatticeElement {

    private int depth = 0;

    Lattice(int numAttributes) {
        super(numAttributes);
    }

    public void addFunctionalDependency(OpenBitSet lhs, int rhs) {
        LatticeElement node = find(lhs, elem -> elem.mark(rhs));
        node.addFd(rhs);
        depth = (int) Math.max(depth, lhs.cardinality());
    }

    private LatticeElement find(OpenBitSet lhs, Consumer<LatticeElement> visitor) {
        LatticeElement currentNode = this;
        visitor.accept(currentNode);
        for (int nextLhsAttribute = lhs.nextSetBit(0); nextLhsAttribute >= 0; nextLhsAttribute = lhs.nextSetBit(nextLhsAttribute + 1)) {
            LatticeElement[] children = currentNode.getChildren();
            if (children == null) {
                children = new LatticeElement[numAttributes];
                currentNode.setChildren(children);
            }
            LatticeElement child = children[nextLhsAttribute];
            if (child == null) {
                child = new LatticeElement(numAttributes);
                children[nextLhsAttribute] = child;
            }
            currentNode = child;
            visitor.accept(currentNode);
        }
        return currentNode;
    }

    public void removeSpecializations(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = 0;
        this.removeSpecializations(lhs, rhs, currentLhsAttr, false);
    }

    public boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = 0;
        return this.containsFdOrGeneralization(lhs, rhs, currentLhsAttr);
    }

    public Collection<LatticeElementLhsPair> getLevel(int level) {
        List<LatticeElementLhsPair> result = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(numAttributes);
        int currentLevel = 0;
        this.getLevel(level, currentLevel, currentLhs, result);
        if (level == depth && result.isEmpty()) {
            depth--;
        }
        return result;
    }

    public int getDepth() {
        return depth;
    }

    public List<OpenBitSetFD> getFunctionalDependencies() {
        List<OpenBitSetFD> functionalDependencies = new ArrayList<>();
        this.addFunctionalDependenciesInto(functionalDependencies, new OpenBitSet(numAttributes));
        return functionalDependencies;
    }

    public void removeFunctionalDependency(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = 0;
        this.removeRecursive(lhs, rhs, currentLhsAttr);
    }

    public List<OpenBitSet> getFdAndGeneralizations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(numAttributes);
        int currentLhsAttr = 0;
        this.getFdAndGeneralizations(lhs, rhs, currentLhsAttr, currentLhs, foundLhs);
        return foundLhs;
    }

}
