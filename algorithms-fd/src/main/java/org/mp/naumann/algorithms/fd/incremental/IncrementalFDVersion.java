package org.mp.naumann.algorithms.fd.incremental;

public enum IncrementalFDVersion {
    V0_0(InsertPruningStrategy.NONE, DeletePruningStrategy.NONE, "Original hyfd version", 0),
    V0_1(InsertPruningStrategy.SIMPLE, DeletePruningStrategy.NONE, "Simple incremental insert pruning", 1),
    V0_2(InsertPruningStrategy.BLOOM, DeletePruningStrategy.NONE, "Improved insert pruning with bloom", 2),
    V0_3(InsertPruningStrategy.SIMPLE, DeletePruningStrategy.ANNOTATION, "Simple pruning and delete annotation",3);


    private final InsertPruningStrategy insertPruningStrategy;
    private final DeletePruningStrategy deletePruningStrategy;
    private final String versionName;
    private final int id;


    public static final IncrementalFDVersion LATEST = V0_3;
    public static final IncrementalFDVersion HYFD_ORIGINAL = V0_0;

    IncrementalFDVersion(InsertPruningStrategy insertPruningStrategy, DeletePruningStrategy deletePruningStrategy, String versionName, int id) {
        this.insertPruningStrategy = insertPruningStrategy;
        this.deletePruningStrategy = deletePruningStrategy;
        this.versionName = versionName;
        this.id = id;
    }


    public String getVersionName() {
        return versionName;
    }

    public int getId() {
        return id;
    }

    public static IncrementalFDVersion valueOf(int shortid){
        for(IncrementalFDVersion ifdv : IncrementalFDVersion.values())
            if(ifdv.id == shortid)
                return ifdv;
        return LATEST; // Fallback to latest
    }

    public InsertPruningStrategy getInsertPruningStrategy() {
        return insertPruningStrategy;
    }

    public DeletePruningStrategy getDeletePruningStrategy() {
        return deletePruningStrategy;
    }

    public enum InsertPruningStrategy {
        NONE, SIMPLE, BLOOM
    }

    public enum DeletePruningStrategy {
        NONE, ANNOTATION
    }

}
