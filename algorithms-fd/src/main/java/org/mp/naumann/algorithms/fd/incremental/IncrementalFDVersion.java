package org.mp.naumann.algorithms.fd.incremental;

public enum IncrementalFDVersion {
    V0_0(PruningStrategy.NONE, "Original hyfd version", 0),
    V0_1(PruningStrategy.SIMPLE, "Simple incremental pruning", 1),
    V0_2(PruningStrategy.BLOOM, "Improved pruning with bloom", 2);


    private final PruningStrategy pruningStrategy;
    private final String versionName;
    private final int shortid;


    public static final IncrementalFDVersion LATEST = V0_2;
    public static final IncrementalFDVersion HYFD_ORIGINAL = V0_0;

    IncrementalFDVersion(PruningStrategy pruningStrategy, String versionName, int shortid) {
        this.pruningStrategy = pruningStrategy;
        this.versionName = versionName;
        this.shortid = shortid;
    }


    public String getVersionName() {
        return versionName;
    }

    public int getShortId() {
        return shortid;
    }

    public PruningStrategy getPruningStrategy() {
        return pruningStrategy;
    }

    public enum PruningStrategy {
        NONE, SIMPLE, BLOOM
    }

    public static IncrementalFDVersion fromShortId(int shortid){
        for(IncrementalFDVersion ifdv : IncrementalFDVersion.values())
            if(ifdv.shortid == shortid)
                return ifdv;
        return LATEST; // Fallback to latest
    }
}
