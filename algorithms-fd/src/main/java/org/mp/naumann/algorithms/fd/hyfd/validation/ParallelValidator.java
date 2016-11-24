package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ParallelValidator extends Validator {

    private final static Logger LOG = Logger.getLogger(ParallelValidator.class.getName());

    private final ExecutorService executor;

    public ParallelValidator(FDSet negCover, FDTree posCover, PliCollection plis, float efficiencyThreshold,
                             MemoryGuardian memoryGuardian) {
        super(negCover, posCover, plis, efficiencyThreshold, memoryGuardian);
        int numThreads = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(numThreads);
    }


    @Override
    protected ValidationResult validate(List<FDTreeElementLhsPair> currentLevel, int level) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        List<Future<ValidationResult>> futures = new ArrayList<>();
        for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
            ValidationTask task = new ValidationTask(getPlis(), level, elementLhsPair);
            futures.add(this.executor.submit(task));
        }

        for (Future<ValidationResult> future : futures) {
            try {
                validationResult.add(future.get());
            } catch (ExecutionException | InterruptedException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    @Override
    public List<IntegerPair> validatePositiveCover() throws AlgorithmExecutionException {
       List<IntegerPair> result = super.validatePositiveCover();
        shutdownExecutor();
        return result;

    }

    private void shutdownExecutor(){
        this.executor.shutdown();
        try {
            this.executor.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
