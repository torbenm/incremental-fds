package org.mp.naumann.algorithms.fd.incremental;

public class IncrementalFDVersion {
    public static IncrementalFDVersion V0_0 = new IncrementalFDVersion(PruningStrategy.NONE, "Original hyfd version", 0);
    public static IncrementalFDVersion V0_1 = new IncrementalFDVersion(PruningStrategy.SIMPLE, "Simple incremental pruning", 1);
    public static IncrementalFDVersion V0_2 = new IncrementalFDVersion(PruningStrategy.BLOOM, "Improved pruning with bloom", 2);
    public static IncrementalFDVersion V0_3 = new IncrementalFDVersion(PruningStrategy.BLOOM_ADVANCED, "Improved pruning with bloom based on initial FDs", 3);


    private final PruningStrategy pruningStrategy;
    private final String versionName;
    private final int id;
    private final boolean sampling;
    private final boolean clusterPruning;

    public static final IncrementalFDVersion LATEST = V0_3;
    public static final IncrementalFDVersion HYFD_ORIGINAL = V0_0;

    IncrementalFDVersion(PruningStrategy pruningStrategy, String versionName, int id) {
        this(pruningStrategy, versionName, id, true, true);
    }

    IncrementalFDVersion(PruningStrategy pruningStrategy, String versionName, int id, boolean sampling, boolean clusterPruning) {
        this.pruningStrategy = pruningStrategy;
        this.versionName = versionName;
        this.id = id;
        this.sampling = sampling;
        this.clusterPruning = clusterPruning;
    }


    public String getVersionName() {
        return versionName;
    }

    public int getId() {
        return id;
    }

    public static IncrementalFDVersion valueOf(int shortid){
        switch (shortid) {
            case 3: return V0_3;
            case 2: return V0_2;
            case 1: return V0_1;
            case 0: return V0_0;
            default: return LATEST;
        }
    }

    public PruningStrategy getPruningStrategy() {
        return pruningStrategy;
    }

    public boolean useSampling() {
        return sampling;
    }

    public boolean useClusterPruning() {
        return clusterPruning;
    }

    public enum PruningStrategy {
        NONE, SIMPLE, BLOOM, BLOOM_ADVANCED
    }

}
