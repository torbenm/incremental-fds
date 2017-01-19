package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class GeneralizingIncrementalValidator {

	private FDSet negCover;
	private FDTree posCover;
	private int numRecords;
	private List<PositionListIndex> plis;
	private int[][] compressedRecords;
	private float efficiencyThreshold;
	private MemoryGuardian memoryGuardian;
	private ExecutorService executor;

	private int level = Integer.MIN_VALUE;
	private int pruned = 0;
	private int validations = 0;
	private final List<ValidationPruner> validationPruners = new ArrayList<>();

	public void addValidationPruner(ValidationPruner ValidationPruner) {
		validationPruners.add(ValidationPruner);
	}

	public GeneralizingIncrementalValidator(FDSet negCover, FDTree posCover, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.numRecords = compressedRecords.length;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
		this.efficiencyThreshold = efficiencyThreshold;
		this.memoryGuardian = memoryGuardian;
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
		public List<FD> validFDs = new ArrayList<>();
		public List<IntegerPair> comparisonSuggestions = new ArrayList<>();

		public void add(ValidationResult other) {
			this.validations += other.validations;
			this.intersections += other.intersections;
			this.validFDs.addAll(other.validFDs);
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

			if (GeneralizingIncrementalValidator.this.level == 0) {
				// Check if rhs is unique
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!GeneralizingIncrementalValidator.this.plis.get(rhsAttr).isConstant(GeneralizingIncrementalValidator.this.numRecords)) {
						element.removeFd(rhsAttr);
					}else{
					    // The fd is valid

                        result.validFDs.add(new FD(lhs, rhsAttr));
                    }
					result.intersections++;
				}
			}
			else if (GeneralizingIncrementalValidator.this.level == 1) {
				// Check if lhs from plis refines rhs
				int lhsAttribute = lhs.nextSetBit(0);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    if (!GeneralizingIncrementalValidator.this.plis.get(lhsAttribute).refines(GeneralizingIncrementalValidator.this.compressedRecords, rhsAttr)) {
						element.removeFd(rhsAttr);
					}else{
                        result.validFDs.add(new FD(lhs, rhsAttr));
                    }
					result.intersections++;
				}
			}
			else {

				// Check if lhs from plis plus remaining inverted plis refines rhs
				int firstLhsAttr = lhs.nextSetBit(0);
				lhs.clear(firstLhsAttr);
				OpenBitSet validRhs = GeneralizingIncrementalValidator.this.plis.get(firstLhsAttr).refines(GeneralizingIncrementalValidator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
				lhs.set(firstLhsAttr);
				
				result.intersections++;

				rhs.andNot(validRhs); // Now contains all invalid FDs
				element.setFds(validRhs); // Sets the valid FDs in the FD tree
				
				for (int rhsAttr = validRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = validRhs.nextSetBit(rhsAttr + 1)) {
				    result.validFDs.add(new FD(lhs, rhsAttr));
                }
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


	
	public boolean validatePositiveCover() throws AlgorithmExecutionException {
		FDLogger.log(Level.FINER, "Validating FDs using plis ...");
        if(level == Integer.MIN_VALUE){
            // First start! set level
            level = posCover.getDepth();
            FDLogger.log(Level.FINER, "Set level to "+posCover.getDepth());
        }
		List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
		// Zoom to first level that actually has some content
        while(currentLevel.size() == 0 && level > 0){
            level--;
            currentLevel = FDTreeUtils.getFdLevel(posCover, level);
        }
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		while (level >= 0) {
			FDLogger.log(Level.FINE, "\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");
			
			// Validate current level
			FDLogger.log(Level.FINER, "(V)");

			validations += currentLevel.size();
			ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
			comparisonSuggestions.addAll(validationResult.comparisonSuggestions);
						

			FDLogger.log(Level.FINER, "(G); ");

			// If we have found a valid FD, this also means that the specialisation of this FD is not minimal
            // Thus we must remove all from the previous level
            clearPreviousLevel(validationResult.validFDs);

            // Generate new FDs from the invalid FDs and add them to the next level as well
            // In contrast to the "Normal" Validator, as we go bottom up, we create the next level out of the valid FDs
            int candidates = generateNextLevel(validationResult.validFDs);

			this.level--;
			int numValidFds = validationResult.validFDs.size();
			int numInvalidFds = validationResult.validations - numValidFds;
			FDLogger.log(Level.FINER, validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");

			// Decide if we continue validating the next level or if we go back into the sampling phase
			if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
				return true;
			currentLevel = FDTreeUtils.getFdLevel(posCover, level);
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
		
		return false;
	}

	private void clearPreviousLevel(List<FD> validFDs){
        validFDs.forEach(this::clearPreviousLevel);
    }

    private void clearPreviousLevel(FD validFD){
	    if(validFD.lhs.cardinality() == 0)
	        return;
	    int previousLevel = this.level + 1; // Get this from the FD maybe instead?
        posCover.getLevel(previousLevel, validFD.lhs).forEach(e -> e.getElement().removeFd(validFD.rhs));
    }


    private int generateNextLevel(List<FD> validFDs){
        return validFDs.stream().mapToInt(this::generateNextLevel).sum();
    }

    private int generateNextLevel(FD validFD){
        if(validFD.lhs.cardinality() == 1)
            return 0;
        int candidates = 0;
        for(int removeLhs = validFD.lhs.nextSetBit(0); removeLhs >= 0; removeLhs =validFD.lhs.nextSetBit(removeLhs+1)){
            OpenBitSet lhs = validFD.lhs.clone();
            lhs.clear(removeLhs);
            candidates += addFunctionalDependencyNoCheck(lhs, validFD.rhs);
        }
        return candidates;
    }

    private int addFunctionalDependency(OpenBitSet lhs, int rhs){
        int candidates = 0;
        if(!posCover.containsFdOrGeneralization(lhs, rhs)){
            // Check if this attribute has already been added
            FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(lhs, rhs);
            if(child != null) {
                candidates++;
            }
            //TODO: Make Memory Guardian cut the bottom, not the top
            //checkMemoryGuardian();
        }
        return candidates;
    }

    private int addFunctionalDependencyNoCheck(OpenBitSet lhs, int rhs){
        int candidates = 0;
        // Check if this attribute has already been added
        FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(lhs, rhs);
        if(child != null) {
            candidates++;
        }
        //TODO: Make Memory Guardian cut the bottom, not the top
        //checkMemoryGuardian();
        return candidates;
    }

    private void checkMemoryGuardian(){
        this.memoryGuardian.memoryChanged(1);
        this.memoryGuardian.match(this.negCover, this.posCover, null);
    }

}
