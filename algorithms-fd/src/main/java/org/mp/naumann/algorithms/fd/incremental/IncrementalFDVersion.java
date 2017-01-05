package org.mp.naumann.algorithms.fd.incremental;

public class IncrementalFDVersion {

    public static IncrementalFDVersion V0_0 = new IncrementalFDVersion(InsertPruningStrategy.NONE, DeletePruningStrategy.NONE, "Original hyfd version", 0);
    public static IncrementalFDVersion V0_1 = new IncrementalFDVersion(InsertPruningStrategy.SIMPLE, DeletePruningStrategy.NONE, "Simple incremental pruning", 1);
    public static IncrementalFDVersion V0_2 = new IncrementalFDVersion(InsertPruningStrategy.BLOOM, DeletePruningStrategy.NONE, "Improved pruning with bloom", 2);
    public static IncrementalFDVersion V0_3 = new IncrementalFDVersion(InsertPruningStrategy.BLOOM_ADVANCED,DeletePruningStrategy.NONE,  "Improved pruning with bloom based on initial FDs", 3);
    public static IncrementalFDVersion V0_4 = new IncrementalFDVersion(InsertPruningStrategy.NONE,DeletePruningStrategy.ANNOTATION,  "Improved pruning with bloom based on initial FDs, annotation pruning for deletes", 4);
    public static IncrementalFDVersion V0_5 = new IncrementalFDVersion(InsertPruningStrategy.BLOOM_ADVANCED,DeletePruningStrategy.ANNOTATION,  "Improved pruning with bloom based on initial FDs, annotation pruning for deletes", 4);
    public static final IncrementalFDVersion LATEST = V0_4;
    public static final IncrementalFDVersion HYFD_ORIGINAL = V0_0;


    private final InsertPruningStrategy insertPruningStrategy;
    private final DeletePruningStrategy deletePruningStrategy;
    private final String versionName;
    private final int id;
    private final boolean sampling;
    private final boolean clusterPruning;





    private IncrementalFDVersion(InsertPruningStrategy insertPruningStrategy, DeletePruningStrategy deletePruningStrategy, String versionName, int id) {
        this(insertPruningStrategy, deletePruningStrategy, versionName, id, true, true);
    }

    private IncrementalFDVersion(InsertPruningStrategy insertPruningStrategy, DeletePruningStrategy deletePruningStrategy, String versionName, int id, boolean sampling, boolean clusterPruning) {
        this.insertPruningStrategy = insertPruningStrategy;
        this.deletePruningStrategy = deletePruningStrategy;
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



    public InsertPruningStrategy getInsertPruningStrategy() {
        return insertPruningStrategy;
    }

    public DeletePruningStrategy getDeletePruningStrategy() {
        return deletePruningStrategy;
    }

    public boolean useSampling() {
        return sampling;
    }

    public boolean useClusterPruning() {
        return clusterPruning;
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

    public enum DeletePruningStrategy {
        NONE, ANNOTATION
    }
    public enum InsertPruningStrategy {
        NONE, SIMPLE, BLOOM, BLOOM_ADVANCED
    }

}
