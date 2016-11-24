package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.structures.plis.PositionListIndex;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FDTree extends FDTreeElement {

	private int depth = 0;
	private int maxDepth;
	
	public FDTree(int numAttributes, int maxDepth) {
		super(numAttributes);
		this.maxDepth = maxDepth;
		this.children = new FDTreeElement[numAttributes];
	}

	public int getDepth() {
		return this.depth;
	}

	public int getMaxDepth() {
		return this.maxDepth;
	}

	@Override
	public String toString() {
		return "[" + this.depth + " depth, " + this.maxDepth + " maxDepth]";
	}

	public void trim(int newDepth) {
		this.trimRecursive(0, newDepth);
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	public void addMostGeneralDependencies() {
		this.rhsAttributes.set(0, this.numAttributes);
		this.rhsFds.set(0, this.numAttributes);
	}

	public FDTreeElement addFunctionalDependency(OpenBitSet lhs, int rhs) {
        return addFunctionalDependency(lhs, this, n -> n.addRhsAttribute(rhs), n -> n.markFd(rhs), false);
	}

	public FDTreeElement addFunctionalDependency(OpenBitSet lhs, OpenBitSet rhs) {
        return addFunctionalDependency(lhs, this, n -> n.addRhsAttributes(rhs), n -> n.markFds(rhs), false);
	}

    public FDTreeElement addFunctionalDependencyGetIfNew(OpenBitSet lhs, int rhs) {
		return addFunctionalDependency(lhs, this, n -> n.addRhsAttribute(rhs), n -> n.markFd(rhs), true);
	}

    private FDTreeElement addFunctionalDependency(OpenBitSet lhs, FDTreeElement startNode,
                                                  Consumer<FDTreeElement> addRhsConsumer, Consumer<FDTreeElement> markFdConsumer,
                                                  boolean onlyNew){
        FDTreeElement currentNode = startNode;
        addRhsConsumer.accept(currentNode);
        int lhsLength = 0;
        boolean isNew = !onlyNew;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;

            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            }
            else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            }

            currentNode = currentNode.getChildren()[i];
            addRhsConsumer.accept(currentNode);
        }
        markFdConsumer.accept(currentNode);
        this.depth = Math.max(this.depth, lhsLength);
        return isNew ? currentNode : null;
    }

	public boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs) {
		int nextLhsAttr = lhs.nextSetBit(0);
		return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
	}

	public List<OpenBitSet> getFdAndGeneralizations(OpenBitSet lhs, int rhs) {
		List<OpenBitSet> foundLhs = new ArrayList<>();
		OpenBitSet currentLhs = new OpenBitSet();
		int nextLhsAttr = lhs.nextSetBit(0);
		this.getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
		return foundLhs;
	}

	public List<FDTreeElementLhsPair> getLevel(int level) {
		List<FDTreeElementLhsPair> result = new ArrayList<>();
		OpenBitSet currentLhs = new OpenBitSet();
		int currentLevel = 0;
		this.getLevel(level, currentLevel, currentLhs, result);
		return result;
	}

	public void removeFunctionalDependency(OpenBitSet lhs, int rhs) {
		int currentLhsAttr = lhs.nextSetBit(0);
		this.removeRecursive(lhs, rhs, currentLhsAttr);
	}
	
	public List<FunctionalDependency> getFunctionalDependencies(ObjectArrayList<ColumnIdentifier> columnIdentifiers, PliCollection plis) {
		List<FunctionalDependency> functionalDependencies = new ArrayList<>();
		this.addFunctionalDependenciesInto(functionalDependencies, new OpenBitSet(), columnIdentifiers, plis);
		return functionalDependencies;
	}
	
	public int addFunctionalDependenciesInto(FunctionalDependencyResultReceiver resultReceiver, ObjectArrayList<ColumnIdentifier> columnIdentifiers, PliCollection plis) {
		return this.addFunctionalDependenciesInto(resultReceiver, new OpenBitSet(), columnIdentifiers, plis);
	}
}
