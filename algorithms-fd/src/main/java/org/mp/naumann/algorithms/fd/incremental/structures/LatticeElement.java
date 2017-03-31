package org.mp.naumann.algorithms.fd.incremental.structures;

import org.apache.lucene.util.OpenBitSet;

import java.util.List;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class LatticeElement {

    final int numAttributes;
    private final OpenBitSet rhsFds;
    private final OpenBitSet markedRhs;
    private LatticeElement[] children;

    LatticeElement(int numAttributes) {
        this.numAttributes = numAttributes;
        this.rhsFds = new OpenBitSet(numAttributes);
        this.markedRhs = new OpenBitSet(numAttributes);
    }

    LatticeElement[] getChildren() {
        return children;
    }

    void setChildren(LatticeElement[] children) {
        this.children = children;
    }

    void addFd(int rhsAttribute) {
        this.rhsFds.fastSet(rhsAttribute);
        this.mark(rhsAttribute);
    }

    public void removeFd(int rhsAttribute) {
        this.rhsFds.fastClear(rhsAttribute);
        this.unmark(rhsAttribute);
    }

    void mark(int rhsAttribute) {
        this.markedRhs.fastSet(rhsAttribute);
    }

    private void unmark(int rhsAttribute) {
        this.markedRhs.fastClear(rhsAttribute);
    }

    boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        if (this.isFd(rhs)) {
            return true;
        }

        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr);
        // Is the dependency already read and we have not yet found a generalization?
        if (nextLhsAttr < 0) {
            return false;
        }

        if ((this.children != null) && (this.children[nextLhsAttr] != null) && (this.children[nextLhsAttr].isMarked(rhs))) {
            if (this.children[nextLhsAttr].containsFdOrGeneralization(lhs, rhs, nextLhsAttr + 1)) {
                return true;
            }
        }

        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr + 1);
    }

    private boolean isMarked(int rhs) {
        return this.markedRhs.fastGet(rhs);
    }

    private boolean isFd(int rhs) {
        return this.rhsFds.fastGet(rhs);
    }

    private boolean isLastNodeOf(int rhs) {
        if (this.children == null) {
            return true;
        }
        for (LatticeElement child : this.children) {
            if ((child != null) && child.isMarked(rhs)) {
                return false;
            }
        }
        return true;
    }

    private boolean hasChildren() {
        for (LatticeElement child : this.children)
            if (child != null)
                return true;
        return false;
    }

    private boolean hasNoMarked() {
        return markedRhs.isEmpty();
    }

    void getLevel(int level, int currentLevel, OpenBitSet currentLhs, List<LatticeElementLhsPair> result) {
        if (level == currentLevel && !rhsFds.isEmpty()) {
            result.add(new LatticeElementLhsPair(currentLhs.clone(), this));
        } else {
            currentLevel++;
            if (this.children == null || this.markedRhs.isEmpty()) {
                return;
            }

            for (int child = currentLevel - 1; child < this.numAttributes; child++) {
                if (this.children[child] == null) {
                    continue;
                }

                currentLhs.fastSet(child);
                this.children[child].getLevel(level, currentLevel, currentLhs, result);
                currentLhs.fastClear(child);
            }
        }
    }

    void removeSpecializations(OpenBitSet lhs, int rhs, int currentAttr, boolean isSpecialized) {
        // If rhs is not marked, we cannot reach any specializatioin from here
        if (!isMarked(rhs)) {
            return;
        }
        int nextLhsAttr = lhs.nextSetBit(currentAttr);
        // If the whole lhs was read and the lhs is specialized, we can remove the rhs
        if (isSpecialized && nextLhsAttr < 0 && isFd(rhs)) {
            this.removeFd(rhs);
            return;
        }

        if ((this.children != null)) {
            int limit = nextLhsAttr;
            if (nextLhsAttr < 0) {
                limit = numAttributes - 1;
            }
            for (int attr = currentAttr; attr <= limit; attr++) {
                if (this.children[attr] != null) {
                    // Move to the next child with the next attribute
                    // Either it is the next lhs attribute or another so we are specialized
                    this.children[attr].removeSpecializations(lhs, rhs, attr + 1, isSpecialized || attr != nextLhsAttr);

                    // Delete the child node if it has no rhsFds attributes any more
                    if (this.children[attr].hasNoMarked()) {
                        this.children[attr] = null;
                    }
                }
            }
            if (!hasChildren())
                children = null;
        }

        // Check if another child requires the rhsFds and if not, remove it from this node
        if (!this.isFd(rhs) && this.isLastNodeOf(rhs)) {
            this.unmark(rhs);
        }
    }

    boolean removeRecursive(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr);
        // If this is the last attribute of lhs, remove the fd-mark from the rhsFds
        if (nextLhsAttr < 0) {
            this.removeFd(rhs);
            return true;
        }

        if ((this.children != null) && (this.children[nextLhsAttr] != null)) {
            // Move to the next child with the next lhs attribute
            if (!this.children[nextLhsAttr].removeRecursive(lhs, rhs, nextLhsAttr + 1)) {
                return false; // This is a shortcut: if the child was unable to remove the rhsFds, then this node can also not remove it
            }

            // Delete the child node if it has no rhsFds attributes any more
            if (this.children[nextLhsAttr].hasNoMarked()) {
                this.children[nextLhsAttr] = null;
            }

            if (!hasChildren())
                children = null;
        }

        // Check if another child requires the rhsFds and if not, remove it from this node
        if (this.isLastNodeOf(rhs)) {
            this.unmark(rhs);
            return true;
        }
        return false;
    }

    public OpenBitSet getRhsFds() {
        return rhsFds;
    }

    void addFunctionalDependenciesInto(List<OpenBitSetFD> functionalDependencies, OpenBitSet lhs) {

        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            functionalDependencies.add(new OpenBitSetFD(lhs.clone(), rhs));
        }

        if (this.getChildren() == null) {
            return;
        }

        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            LatticeElement child = this.getChildren()[childAttr];
            if (child != null) {
                lhs.fastSet(childAttr);
                child.addFunctionalDependenciesInto(functionalDependencies, lhs);
                lhs.fastClear(childAttr);
            }
        }
    }

    void getFdAndGeneralizations(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs,
                                 List<OpenBitSet> foundLhs) {
        if (this.isFd(rhs)) {
            foundLhs.add(currentLhs.clone());
            return;
        }

        if (this.children == null) {
            return;
        }

        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr);
        while (nextLhsAttr >= 0) {

            if ((this.children[nextLhsAttr] != null) && (this.children[nextLhsAttr].isMarked(rhs))) {
                currentLhs.fastSet(nextLhsAttr);
                this.children[nextLhsAttr].getFdAndGeneralizations(lhs, rhs, nextLhsAttr + 1, currentLhs, foundLhs);
                currentLhs.fastClear(nextLhsAttr);
            }

            nextLhsAttr = lhs.nextSetBit(nextLhsAttr + 1);
        }
    }

}
