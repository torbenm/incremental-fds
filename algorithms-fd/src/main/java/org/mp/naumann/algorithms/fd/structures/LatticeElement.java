package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;

import java.util.List;

public class LatticeElement {

    final int numAttributes;
    private LatticeElement[] children;
    private final OpenBitSet rhsFds;
    private final OpenBitSet markedRhs;

    LatticeElement(int numAttributes) {
        this.numAttributes = numAttributes;
        this.rhsFds = new OpenBitSet(numAttributes);
        this.markedRhs = new OpenBitSet(numAttributes);
    }

    public LatticeElement[] getChildren() {
        return children;
    }

    void setChildren(LatticeElement[] children) {
        this.children = children;
    }

    void addFd(int rhsAttribute) {
        this.rhsFds.fastSet(rhsAttribute);
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

        // Is the dependency already read and we have not yet found a generalization?
        if (currentLhsAttr < 0) {
            return false;
        }

        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

        if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].isMarked(rhs))) {
            if (this.children[currentLhsAttr].containsFdOrGeneralization(lhs, rhs, nextLhsAttr)) {
                return true;
            }
        }

        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
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
            if (this.children == null) {
                return;
            }

            for (int child = 0; child < this.numAttributes; child++) {
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
        if (isSpecialized && nextLhsAttr < 0) {
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
        // If this is the last attribute of lhs, remove the fd-mark from the rhsFds
        if (currentLhsAttr < 0) {
            this.removeFd(rhs);
            return true;
        }

        if ((this.children != null) && (this.children[currentLhsAttr] != null)) {
            // Move to the next child with the next lhs attribute
            if (!this.children[currentLhsAttr].removeRecursive(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1))) {
                return false; // This is a shortcut: if the child was unable to remove the rhsFds, then this node can also not remove it
            }

            // Delete the child node if it has no rhsFds attributes any more
            if (this.children[currentLhsAttr].hasNoMarked()) {
                this.children[currentLhsAttr] = null;
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
        }

        if (this.children == null) {
            return;
        }

        while (currentLhsAttr >= 0) {
            int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

            if ((this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].isMarked(rhs))) {
                currentLhs.fastSet(currentLhsAttr);
                this.children[currentLhsAttr].getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
                currentLhs.fastClear(currentLhsAttr);
            }

            currentLhsAttr = nextLhsAttr;
        }
    }

    public OpenBitSet getMarkedRhs() {
        return markedRhs;
    }
}
