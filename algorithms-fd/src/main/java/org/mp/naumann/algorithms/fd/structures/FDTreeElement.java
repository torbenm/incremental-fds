package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FDTreeElement {

    FDTreeElement[] children;
    OpenBitSet rhsAttributes;
    OpenBitSet rhsFds;
    int numAttributes;


    FDTreeElement(int numAttributes) {
        this.rhsAttributes = new OpenBitSet(numAttributes);
        this.rhsFds = new OpenBitSet(numAttributes);
        this.numAttributes = numAttributes;
    }

    public int getNumAttributes() {
        return this.numAttributes;
    }

    // children

    public FDTreeElement[] getChildren() {
        return this.children;
    }

    void setChildren(FDTreeElement[] children) {
        this.children = children;
    }

    // rhsAttributes

    private OpenBitSet getRhsAttributes() {
        return this.rhsAttributes;
    }

    void addRhsAttribute(int i) {
        this.rhsAttributes.set(i);
    }

    void addRhsAttributes(OpenBitSet other) {
        this.rhsAttributes.or(other);
    }

    private void removeRhsAttribute(int i) {
        this.rhsAttributes.clear(i);
    }

    private boolean hasRhsAttribute(int i) {
        return this.rhsAttributes.get(i);
    }

    // rhsFds

    public OpenBitSet getFds() {
        return this.rhsFds;
    }

    void markFd(int i) {
        this.rhsFds.set(i);
    }

    void markFds(OpenBitSet other) {
        this.rhsFds.or(other);
    }

    public void removeFd(int i) {
        this.rhsFds.clear(i);
    }

    public void setFds(OpenBitSet other) {
        this.rhsFds = other;
    }

    public boolean isFd(int i) {
        return this.rhsFds.get(i);
    }

    public Collection<Integer> getFdCollection(){
        List<Integer> fds = new ArrayList<>();
        for(int rhsAttr = rhsAttributes.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhsAttributes.nextSetBit(rhsAttr+1)){
            fds.add(rhsAttr);
        }
        return fds;
    }

    public Collection<FunctionalDependency> getFdCollection(OpenBitSet lhs, ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<IPositionListIndex> plis){
        List<FunctionalDependency> fds = new ArrayList<>();
        for(int rhsAttr = rhsAttributes.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhsAttributes.nextSetBit(rhsAttr+1)){
            fds.add(findFunctionDependency(lhs, rhsAttr, columnIdentifiers, plis));
        }
        return fds;
    }

    void trimRecursive(int currentDepth, int newDepth) {
        if (currentDepth == newDepth) {
            this.children = null;
            this.rhsAttributes.and(this.rhsFds);
            return;
        }

        if (this.children != null)
            for (FDTreeElement child : this.children)
                if (child != null)
                    child.trimRecursive(currentDepth + 1, newDepth);
    }

    boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        if (this.isFd(rhs))
            return true;

        // Is the dependency already read and we have not yet found a generalization?
        if (currentLhsAttr < 0)
            return false;

        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

        if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs)))
            if (this.children[currentLhsAttr].containsFdOrGeneralization(lhs, rhs, nextLhsAttr))
                return true;

        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
    }

    void getFdAndGeneralizations(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs,
                                 List<OpenBitSet> foundLhs) {
        if (this.isFd(rhs))
            foundLhs.add(currentLhs.clone());

        if (this.children == null)
            return;

        while (currentLhsAttr >= 0) {
            int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

            if ((this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs))) {
                currentLhs.set(currentLhsAttr);
                this.children[currentLhsAttr].getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
                currentLhs.clear(currentLhsAttr);
            }

            currentLhsAttr = nextLhsAttr;
        }
    }

    void getLevel(int level, int currentLevel, OpenBitSet currentLhs, List<FDTreeElementLhsPair> result) {
        if (level == currentLevel) {
            result.add(new FDTreeElementLhsPair(this, currentLhs.clone()));
        } else {
            currentLevel++;
            if (this.children == null)
                return;

            for (int child = 0; child < this.numAttributes; child++) {
                if (this.children[child] == null)
                    continue;

                currentLhs.set(child);
                this.children[child].getLevel(level, currentLevel, currentLhs, result);
                currentLhs.clear(child);
            }
        }
    }

    boolean removeRecursive(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        // If this is the last attribute of lhs, remove the fd-mark from the rhs
        if (currentLhsAttr < 0) {
            this.removeFd(rhs);
            this.removeRhsAttribute(rhs);
            return true;
        }

        if ((this.children != null) && (this.children[currentLhsAttr] != null)) {
            // Move to the next child with the next lhs attribute
            if (!this.children[currentLhsAttr].removeRecursive(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1)))
                return false; // This is a shortcut: if the child was unable to remove the rhs, then this node can also not remove it

            // Delete the child node if it has no rhs attributes any more
            if (this.children[currentLhsAttr].getRhsAttributes().cardinality() == 0)
                this.children[currentLhsAttr] = null;
        }

        // Check if another child requires the rhs and if not, remove it from this node
        if (this.isLastNodeOf(rhs)) {
            this.removeRhsAttribute(rhs);
            return true;
        }
        return false;
    }
    public void removeChildren(int rhs){
        if(this.children == null) return;

        for(int i = 0; i < numAttributes; i++){
            if(this.children[i] != null && this.children[i].isFd(rhs)){
                this.children[i].removeRhsAttribute(rhs);
            }
        }
    }

    private boolean isLastNodeOf(int rhs) {
        if (this.children == null)
            return true;
        for (FDTreeElement child : this.children)
            if ((child != null) && child.hasRhsAttribute(rhs))
                return false;
        return true;
    }

    void addFunctionalDependenciesInto(List<FunctionalDependency> functionalDependencies, OpenBitSet lhs,
                                       ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<? extends IPositionListIndex> plis) {

        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            FunctionalDependency fdResult =  findFunctionDependency(lhs, rhs, columnIdentifiers, plis);
            functionalDependencies.add(fdResult);
        }

        if (this.getChildren() == null)
            return;

        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            FDTreeElement element = this.getChildren()[childAttr];
            if (element != null) {
                lhs.set(childAttr);
                element.addFunctionalDependenciesInto(functionalDependencies, lhs, columnIdentifiers, plis);
                lhs.clear(childAttr);
            }
        }
    }

    int addFunctionalDependenciesInto(FunctionalDependencyResultReceiver resultReceiver, OpenBitSet lhs,
                                      ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<? extends IPositionListIndex> plis) {
        int numFDs = 0;
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            FunctionalDependency fdResult = findFunctionDependency(lhs, rhs, columnIdentifiers, plis);
            resultReceiver.receiveResult(fdResult);
            numFDs++;
        }

        if (this.getChildren() == null)
            return numFDs;

        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            FDTreeElement element = this.getChildren()[childAttr];
            if (element != null) {

                lhs.set(childAttr);
                numFDs += element.addFunctionalDependenciesInto(resultReceiver, lhs, columnIdentifiers, plis);
                lhs.clear(childAttr);
            }
        }

        return numFDs;
    }

    private FunctionalDependency findFunctionDependency(OpenBitSet lhs, int rhs,
                                                        ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<? extends IPositionListIndex> plis){
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) lhs.cardinality()];
        int j = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
            columns[j++] = columnIdentifiers.get(columnId);
        }

        ColumnCombination colCombination = new ColumnCombination(columns);
        int rhsId = plis.get(rhs).getAttribute(); // Here we translate the column rhs back to the real column rhs before the sorting
        return new FunctionalDependency(colCombination, columnIdentifiers.get(rhsId));
    }
}
