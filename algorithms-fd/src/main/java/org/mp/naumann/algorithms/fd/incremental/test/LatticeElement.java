package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.List;

public class LatticeElement {

    protected final int numAttributes;
    private LatticeElement[] children;
    private final OpenBitSet rhs;
    private final OpenBitSet markedRhs;

    LatticeElement(int numAttributes) {
        this.numAttributes = numAttributes;
        this.rhs = new OpenBitSet(numAttributes);
        this.markedRhs = new OpenBitSet(numAttributes);
    }

    public LatticeElement[] getChildren() {
        return children;
    }

    void setChildren(LatticeElement[] children) {
        this.children = children;
    }

    void addFd(int rhsAttribute) {
        this.rhs.fastSet(rhsAttribute);
    }

    void removeFd(int rhsAttribute) {
        this.rhs.fastClear(rhsAttribute);
        this.unmark(rhsAttribute);
    }

    void mark(int rhsAttribute) {
        this.markedRhs.fastSet(rhsAttribute);
    }

    private void unmark(int rhsAttribute) {
        this.markedRhs.fastClear(rhsAttribute);
    }

    boolean containsFdOrGeneralization(OpenBitSetFD fd, int currentLhsAttr) {
        if (this.isFd(fd.getRhs())) {
            return true;
        }

        // Is the dependency already read and we have not yet found a generalization?
        if (currentLhsAttr < 0) {
            return false;
        }

        OpenBitSet lhs = fd.getLhs();
        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

        if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].isMarked(fd.getRhs()))) {
            if (this.children[currentLhsAttr].containsFdOrGeneralization(fd, nextLhsAttr)) {
                return true;
            }
        }

        return this.containsFdOrGeneralization(fd, nextLhsAttr);
    }

    private boolean isMarked(int rhs) {
        return this.markedRhs.fastGet(rhs);
    }

    private boolean isFd(int rhs) {
        return this.rhs.fastGet(rhs);
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

    private boolean hasNoMarked() {
        return markedRhs.isEmpty();
    }

    void getLevel(int level, int currentLevel, OpenBitSet currentLhs, List<LhsRhsPair> result) {
        if (level == currentLevel && !rhs.isEmpty()) {
            result.add(new LhsRhsPair(currentLhs.clone(), this));
        } else {
            currentLevel++;
            if (this.children == null) {
                return;
            }

            for (int child = 0; child < this.numAttributes; child++) {
                if (this.children[child] == null) {
                    continue;
                }

                currentLhs.set(child);
                this.children[child].getLevel(level, currentLevel, currentLhs, result);
                currentLhs.clear(child);
            }
        }
    }

    void removeSpecializations(OpenBitSetFD fd, int currentAttr, boolean isSpecialized) {
        // If this is the last attribute of lhs, remove the fd-mark from the rhs
        int nextLhsAttr = fd.getLhs().nextSetBit(currentAttr);
        if (isSpecialized && nextLhsAttr < 0) {
            this.removeFd(fd.getRhs());
            return;
        }

        if ((this.children != null)) {
            int limit = nextLhsAttr;
            if(nextLhsAttr < 0) {
                limit = numAttributes - 1;
            }
            for (int attr = currentAttr; attr <= limit; attr++) {
                if (this.children[attr] != null) {
                    // Move to the next child with the next lhs attribute
                    this.children[attr].removeSpecializations(fd, attr + 1, isSpecialized || attr != nextLhsAttr);

                    // Delete the child node if it has no rhs attributes any more
                    if (this.children[attr].hasNoMarked()) {
                        this.children[attr] = null;
                    }
                }
            }
        }

        // Check if another child requires the rhs and if not, remove it from this node
        if (!this.isFd(fd.getRhs()) && this.isLastNodeOf(fd.getRhs())) {
            this.unmark(fd.getRhs());
        }
    }

    public OpenBitSet getRhs() {
        return rhs;
    }

    void addFunctionalDependenciesInto(List<OpenBitSetFD> functionalDependencies, OpenBitSet lhs) {

        for (int rhs = this.rhs.nextSetBit(0); rhs >= 0; rhs = this.rhs.nextSetBit(rhs + 1)) {
            functionalDependencies.add(new OpenBitSetFD(lhs.clone(), rhs));
        }

        if (this.getChildren() == null)
            return;

        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            LatticeElement element = this.getChildren()[childAttr];
            if (element != null) {
                lhs.set(childAttr);
                element.addFunctionalDependenciesInto(functionalDependencies, lhs);
                lhs.clear(childAttr);
            }
        }
    }
}
