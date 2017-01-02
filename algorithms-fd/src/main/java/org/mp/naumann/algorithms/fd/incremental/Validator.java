package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Validator {

	private FDSet negCover;
	private FDTree posCover;
	private int numRecords;
	private List<PositionListIndex> plis;
	private int[][] compressedRecords;
	private float efficiencyThreshold;
	private MemoryGuardian memoryGuardian;
	private ExecutorService executor;

	private int level = 0;
	private int pruned = 0;
	private int validations = 0;
	private final IncrementalFD alg;
	private final List<PruningStrategy> pruningStrategies = new ArrayList<>();

	public void addPruningStrategy(PruningStrategy pruningStrategy) {
		pruningStrategies.add(pruningStrategy);
	}

	public Validator(FDSet negCover, FDTree posCover, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian, IncrementalFD alg) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.numRecords = compressedRecords.length;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
		this.efficiencyThreshold = efficiencyThreshold;
		this.memoryGuardian = memoryGuardian;
		this.alg = alg;
		
		if (parallel) {
			int numThreads = Runtime.getRuntime().availableProcessors();
			this.executor = Executors.newFixedThreadPool(numThreads);
		}
	}

	public int getValidations() {
		return validations;
	}

	public int getPruned() {
		return pruned;
	}

	private class FD {
		public OpenBitSet lhs;
		public int rhs;
		public FD(OpenBitSet lhs, int rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}
	}
	
	private class ValidationResult {
		public int validations = 0;
		public int intersections = 0;
		public List<FD> invalidFDs = new ArrayList<>();
		public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		public void add(ValidationResult other) {
			this.validations += other.validations;
			this.intersections += other.intersections;
			this.invalidFDs.addAll(other.invalidFDs);
			this.comparisonSuggestions.addAll(other.comparisonSuggestions);
		}
	}
	
	private class ValidationTask implements Callable<ValidationResult> {
		private FDTreeElementLhsPair elementLhsPair;
		public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
			this.elementLhsPair = elementLhsPair;
		}
		public ValidationTask(FDTreeElementLhsPair elementLhsPair) {
			this.elementLhsPair = elementLhsPair;
		}
		public ValidationResult call() throws Exception {
			ValidationResult result = new ValidationResult();
			
			FDTreeElement element = this.elementLhsPair.getElement();
			OpenBitSet lhs = this.elementLhsPair.getLhs();
			OpenBitSet rhs = element.getFds();
			
			int rhsSize = (int) rhs.cardinality();
			if (rhsSize == 0)
				return result;
			result.validations = result.validations + rhsSize;
			
			if (Validator.this.level == 0) {
				// Check if rhs is unique
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else if (Validator.this.level == 1) {
				// Check if lhs from plis refines rhs
				int lhsAttribute = lhs.nextSetBit(0);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!Validator.this.plis.get(lhsAttribute).refines(Validator.this.compressedRecords, rhsAttr)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else {
				// Check if lhs from plis plus remaining inverted plis refines rhs
				int firstLhsAttr = lhs.nextSetBit(0);
				
				lhs.clear(firstLhsAttr);
				OpenBitSet validRhs = Validator.this.plis.get(firstLhsAttr).refines(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
				lhs.set(firstLhsAttr);
				
				result.intersections++;
				
				rhs.andNot(validRhs); // Now contains all invalid FDs
				element.setFds(validRhs); // Sets the valid FDs in the FD tree
				
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
					result.invalidFDs.add(new FD(lhs, rhsAttr));
			}
			return result;
		}
	}

	private ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
		ValidationResult validationResult = new ValidationResult();
		
		ValidationTask task = new ValidationTask(null);
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			task.setElementLhsPair(elementLhsPair);
			try {
				validationResult.add(task.call());
			}
			catch (Exception e) {
				e.printStackTrace();
				throw new AlgorithmExecutionException(e.getMessage());
			}
		}
		
		return validationResult;
	}
	
	private ValidationResult validateParallel(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
		ValidationResult validationResult = new ValidationResult();
		
		List<Future<ValidationResult>> futures = new ArrayList<>();
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			ValidationTask task = new ValidationTask(elementLhsPair);
			futures.add(this.executor.submit(task));
		}
		
		for (Future<ValidationResult> future : futures) {
			try {
				validationResult.add(future.get());
			}
			catch (ExecutionException e) {
				this.executor.shutdownNow();
				e.printStackTrace();
				throw new AlgorithmExecutionException(e.getMessage());
			}
			catch (InterruptedException e) {
				this.executor.shutdownNow();
				e.printStackTrace();
				throw new AlgorithmExecutionException(e.getMessage());
			}
		}
		
		return validationResult;
	}
	
	public List<IntegerPair> validatePositiveCover() throws AlgorithmExecutionException {
		int numAttributes = this.plis.size();
		
		FDLogger.log(Level.FINER, "Validating FDs using plis ...");

		List<FDTreeElementLhsPair> currentLevel = pruneLevel(FDTreeUtils.getFdLevel(posCover, level));
		
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		while (level <= posCover.getDepth()) {
			FDLogger.log(Level.FINE, "\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");
			
			// Validate current level
			FDLogger.log(Level.FINER, "(V)");

			validations += currentLevel.size();
			ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
			comparisonSuggestions.addAll(validationResult.comparisonSuggestions);
			
			// If the next level exceeds the predefined maximum lhs size, then we can stop here
			if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth())) {
				int numInvalidFds = validationResult.invalidFDs.size();
				int numValidFds = validationResult.validations - numInvalidFds;
				FDLogger.log(Level.FINER, "(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
				break;
			}
						
			// Generate new FDs from the invalid FDs and add them to the next level as well
			FDLogger.log(Level.FINER, "(G); ");
			
			int candidates = 0;
			for (FD invalidFD : validationResult.invalidFDs) {
				for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
					OpenBitSet childLhs = this.extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
					if (childLhs != null) {
						FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
						if (child != null) {
							candidates++;
							
							this.memoryGuardian.memoryChanged(1);
							this.memoryGuardian.match(this.negCover, this.posCover, null);
						}
					}
				}
				
				if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth()))
					break;
			}

			this.level++;
			int numInvalidFds = validationResult.invalidFDs.size();
			int numValidFds = validationResult.validations - numInvalidFds;
			FDLogger.log(Level.FINER, validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");
		
			// Decide if we continue validating the next level or if we go back into the sampling phase
			if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
				return comparisonSuggestions;
			currentLevel = pruneLevel(FDTreeUtils.getFdLevel(posCover, level));
			previousNumInvalidFds = numInvalidFds;
		}
		
		if (this.executor != null) {
			this.executor.shutdown();
			try {
				this.executor.awaitTermination(365, TimeUnit.DAYS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

	protected List<FDTreeElementLhsPair> pruneLevel(List<FDTreeElementLhsPair> lvl) {
		List<FDTreeElementLhsPair> currentLevel = new ArrayList<>();
		for (FDTreeElementLhsPair fd : lvl) {
			if (pruningStrategies.stream().anyMatch(ps -> ps.cannotBeViolated(fd))) {
				pruned++;
			} else {
				currentLevel.add(fd);
			}
		}
		FDLogger.log(Level.FINEST, "Will validate: ");
		currentLevel.stream().map(this::toFds).flatMap(Collection::stream)
				.forEach(v -> FDLogger.log(Level.FINEST, v.toString()));
		return currentLevel;
	}

	private OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
		if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
			(rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
			this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
			((this.posCover.getChildren() != null) && (this.posCover.getChildren()[extensionAttr] != null) && this.posCover.getChildren()[extensionAttr].isFd(rhs)))	
																				// Pruning: If B->C, then AB->C cannot be minimal
			return null;
		
		OpenBitSet childLhs = lhs.clone(); // TODO: This clone() could be avoided when done externally
		childLhs.set(extensionAttr);
		
		// TODO: Add more pruning here
		
		// if contains FD: element was a child before and has already been added to the next level
		// if contains Generalization: element cannot be minimal, because generalizations have already been validated
		if (this.posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
			return null;
		
		return childLhs;
	}

	private List<FunctionalDependency> toFds(FDTreeElementLhsPair fd) {
		OpenBitSet lhs = fd.getLhs();
		OpenBitSet rhsFds = fd.getElement().getFds();
		List<FunctionalDependency> fds = new ArrayList<>();
		for (int rhs = rhsFds.nextSetBit(0); rhs >= 0; rhs = rhsFds.nextSetBit(rhs + 1)) {
			FunctionalDependency fdResult = findFunctionDependency(lhs, rhs, alg.buildColumnIdentifiers(),
					plis);
			fds.add(fdResult);
		}
		return fds;
	}

	private FunctionalDependency findFunctionDependency(OpenBitSet lhs, int rhs,
														ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
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
