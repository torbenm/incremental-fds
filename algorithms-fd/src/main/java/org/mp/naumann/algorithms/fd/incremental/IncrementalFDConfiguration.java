package org.mp.naumann.algorithms.fd.incremental;

import java.util.ArrayList;
import java.util.Collection;

public class IncrementalFDConfiguration {
    public static final IncrementalFDConfiguration V0_0 = new IncrementalFDConfiguration("Original hyfd version");
    public static final IncrementalFDConfiguration V0_1 = new IncrementalFDConfiguration("Simple incremental pruning").addPruningStrategy(PruningStrategy.SIMPLE);
    public static final IncrementalFDConfiguration V0_2 = new IncrementalFDConfiguration("Improved pruning with bloom").addPruningStrategy(PruningStrategy.BLOOM);
    public static final IncrementalFDConfiguration V0_3 = new IncrementalFDConfiguration("Improved pruning with bloom based on initial FDs").addPruningStrategy(PruningStrategy.BLOOM_ADVANCED);


    private final Collection<PruningStrategy> pruningStrategies = new ArrayList<>();
    private final String versionName;
    private boolean sampling = false;
    private boolean clusterPruning = true;
    private boolean innerClusterPruning = false;
    private boolean recomputeDataStructures = true;

    public static final IncrementalFDConfiguration LATEST = V0_3;
    public static final IncrementalFDConfiguration HYFD_ORIGINAL = V0_0;

    public IncrementalFDConfiguration(String versionName) {
        this.versionName = versionName;
    }

    public String getVersionName() {
        return versionName;
    }

    public static IncrementalFDConfiguration getVersion(String name){
        switch (name) {
            case "3": return V0_3;
            case "2": return V0_2;
            case "1": return V0_1;
            case "0": return V0_0;
            default: return new IncrementalFDConfiguration(name);
        }
    }

    public Collection<PruningStrategy> getPruningStrategies() {
        return pruningStrategies;
    }

    public boolean usesSampling() {
        return sampling;
    }

    public boolean usesClusterPruning() {
        return clusterPruning;
    }

    public boolean recomputesDataStructures() {
        return recomputeDataStructures;
    }

    public IncrementalFDConfiguration addPruningStrategy(PruningStrategy pruningStrategy) {
        this.pruningStrategies.add(pruningStrategy);
        return this;
    }

    public IncrementalFDConfiguration setRecomputeDataStructures(boolean recomputeDataStructures) {
        this.recomputeDataStructures = recomputeDataStructures;
        return this;
    }

    public IncrementalFDConfiguration recomputeDataStructures() {
        return setRecomputeDataStructures(true);
    }

    public IncrementalFDConfiguration computeDataStructuresIncrementally() {
        return setRecomputeDataStructures(false);
    }

    public IncrementalFDConfiguration enableClusterPruning() {
        return setClusterPruning(true);
    }

    public IncrementalFDConfiguration disableClusterPruning() {
        return setClusterPruning(true);
    }

    public IncrementalFDConfiguration setClusterPruning(boolean clusterPruning) {
        this.clusterPruning = clusterPruning;
        return this;
    }

    public IncrementalFDConfiguration enableSampling() {
        return setSampling(true);
    }

    public IncrementalFDConfiguration disableSampling() {
        return setSampling(true);
    }

    public IncrementalFDConfiguration setSampling(boolean sampling) {
        this.sampling = sampling;
        return this;
    }

    public boolean usesInnerClusterPruning() {
        return innerClusterPruning;
    }

    public IncrementalFDConfiguration setInnerClusterPruning(boolean innerClusterPruning) {
        this.innerClusterPruning = innerClusterPruning;
        return this;
    }

    public enum PruningStrategy {
        SIMPLE, BLOOM, BLOOM_ADVANCED
    }

}
