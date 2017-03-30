package org.mp.naumann.algorithms.fd.tane;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;
import org.mp.naumann.database.data.Row;

import java.util.List;

public class TaneAlgorithm implements FunctionalDependencyAlgorithm {


    private Table table;
    private String tableName;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private FunctionalDependencyResultReceiver fdResultReceiver;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;
    private LongBigArrayBigList tTable;

    public TaneAlgorithm(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        configure(table, resultReceiver);
    }

    public TaneAlgorithm() {

    }

    public void configure(Table table, FunctionalDependencyResultReceiver resultReceiver) {
        this.table = table;
        this.fdResultReceiver = resultReceiver;
    }

    public void execute() {

        level0 = new Object2ObjectOpenHashMap<>();
        level1 = new Object2ObjectOpenHashMap<>();
        prefix_blocks = new Object2ObjectOpenHashMap<>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0
        CombinationHelper chLevel0 = new CombinationHelper();
        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        chLevel0.setPartition(spLevel0);
        spLevel0 = null;
        level0.put(new OpenBitSet(), chLevel0);
        chLevel0 = null;

        // Initialize Level 1
        for (int i = 1; i <= numberAttributes; i++) {
            OpenBitSet combinationLevel1 = new OpenBitSet();
            combinationLevel1.set(i);

            CombinationHelper chLevel1 = new CombinationHelper();
            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);

            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
            chLevel1.setPartition(spLevel1);

            level1.put(combinationLevel1, chLevel1);
        }
        partitions = null;

        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
            // compute dependencies for a level
            computeDependencies();

            // prune the search space
            prune();

            // compute the combinations for the next level
            generateNextLevel();
            l++;
        }
    }

    /**
     * Loads the data from the database or a csv file and creates for each
     * attribute a HashMap, which maps the values to a List of tuple ids.
     *
     * @return A ObjectArrayList with the HashMaps.
     */
    private ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() {
        TableInput input = null;
        if (this.table != null) {
            try {
                input = this.table.open();
            } catch (InputReadException e) {
                return new ObjectArrayList<>(0);
            }
        }
        this.numberAttributes = input.numberOfColumns();
        this.tableName = input.getName();
        this.columnNames = input.getColumnNames();
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = new ObjectArrayList<>(
                this.numberAttributes);
        for (int i = 0; i < this.numberAttributes; i++) {
            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<>();
            partitions.add(partition);
        }
        long tupleId = 0;
        while (input.hasNext()) {
            Row row = input.next();
            for (int i = 0; i < this.numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                String entry = row.getValue(i);
                if (partition.containsKey(entry)) {
                    partition.get(entry).add(tupleId);
                } else {
                    LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                    newEqClass.add(tupleId);
                    partition.put(entry, newEqClass);
                }
            }
            tupleId++;
        }
        this.numberTuples = tupleId;
        return partitions;
    }

    /**
     * Initialize Cplus (resp. rhsCandidates) for each combination of the level.
     */
    private void initializeCplusForLevel() {
        for (OpenBitSet X : level1.keySet()) {

            ObjectArrayList<OpenBitSet> CxwithoutA_list = new ObjectArrayList<>();

            // clone of X for usage in the following loop
            OpenBitSet Xclone = X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);
                OpenBitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                CxwithoutA_list.add(CxwithoutA);
                Xclone.set(A);
            }

            OpenBitSet CforX = new OpenBitSet();

            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (OpenBitSet CxwithoutA : CxwithoutA_list) {
                    CforX.and(CxwithoutA);
                }
            }

            CombinationHelper ch = level1.get(X);
            ch.setRhsCandidates(CforX);
        }
    }

    /**
     * Computes the dependencies for the current level (level1).
     */
    private void computeDependencies() {
        initializeCplusForLevel();

        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {
            if (level1.get(X).isValid()) {
                // Build the intersection between X and C_plus(X)
                OpenBitSet C_plus = level1.get(X).getRhsCandidates();
                OpenBitSet intersection = X.clone();
                intersection.intersect(C_plus);

                // clone of X for usage in the following loop
                OpenBitSet Xclone = X.clone();

                // iterate through all elements (A) of the intersection
                for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                    Xclone.clear(A);

                    // check if X\A -> A is valid
                    StrippedPartition spXwithoutA = level0.get(Xclone).getPartition();
                    StrippedPartition spX = level1.get(X).getPartition();

                    if (spX.getError() == spXwithoutA.getError()) {
                        // found Dependency
                        OpenBitSet XwithoutA = Xclone.clone();
                        processFunctionalDependency(XwithoutA, A);

                        // remove A from C_plus(X)
                        level1.get(X).getRhsCandidates().clear(A);

                        // remove all B in R\X from C_plus(X)
                        OpenBitSet RwithoutX = new OpenBitSet();
                        // set to R
                        RwithoutX.set(1, numberAttributes + 1);
                        // remove X
                        RwithoutX.andNot(X);

                        for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                            level1.get(X).getRhsCandidates().clear(i);
                        }

                    }
                    Xclone.set(A);
                }
            }
        }
    }

    /**
     * Prune the current level (level1) by removing all elements with no rhs
     * candidates. All keys are marked as invalid. In case a key is found,
     * minimal dependencies are added to the result receiver.
     *
     * @throws AlgorithmExecutionException if the result receiver cannot handle the functional
     *                                     dependency.
     */
    private void prune() {
        ObjectArrayList<OpenBitSet> elementsToRemove = new ObjectArrayList<>();
        for (OpenBitSet x : level1.keySet()) {
            if (level1.get(x).getRhsCandidates().isEmpty()) {
                elementsToRemove.add(x);
                continue;
            }
            // Check if x is a key. Thats the case, if the error is 0.
            // See definition of the error on page 104 of the TANE-99 paper.
            if (level1.get(x).isValid() && level1.get(x).getPartition().getError() == 0) {

                // C+(X)\X
                OpenBitSet rhsXwithoutX = level1.get(x).getRhsCandidates().clone();
                rhsXwithoutX.andNot(x);
                for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
                    OpenBitSet intersect = new OpenBitSet();
                    intersect.set(1, numberAttributes + 1);

                    OpenBitSet xUnionAWithoutB = x.clone();
                    xUnionAWithoutB.set(a);
                    for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
                        xUnionAWithoutB.clear(b);
                        CombinationHelper ch = level1.get(xUnionAWithoutB);
                        if (ch != null) {
                            intersect.and(ch.getRhsCandidates());
                        } else {
                            intersect = new OpenBitSet();
                            break;
                        }
                        xUnionAWithoutB.set(b);
                    }

                    if (intersect.get(a)) {
                        OpenBitSet lhs = x.clone();
                        processFunctionalDependency(lhs, a);
                        level1.get(x).getRhsCandidates().clear(a);
                        level1.get(x).setInvalid();
                    }
                }
            }
        }
        for (OpenBitSet x : elementsToRemove) {
            level1.remove(x);
        }
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the
     * dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     */
    private void processFunctionalDependency(OpenBitSet lhs, int a) {
        addDependencyToResultReceiver(lhs, a);
    }

    /**
     * Calculate the product of two stripped partitions and return the result as
     * a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    private StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id) != -1) {
                    partition.get(tTable.get(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId) != -1) {
                    if (partition.get(tTable.get(tId)).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable.get(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable.get(tId), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
    }

    private long getLastSetBitIndex(OpenBitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }

    /**
     * Get prefix of OpenBitSet by copying it and removing the last Bit.
     *
     * @return A new OpenBitSet, where the last set Bit is cleared.
     */
    private OpenBitSet getPrefix(OpenBitSet bitset) {
        OpenBitSet prefix = bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }

    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as the value.
     */
    private void buildPrefixBlocks() {
        this.prefix_blocks.clear();
        for (OpenBitSet level_iter : level0.keySet()) {
            OpenBitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ObjectArrayList<OpenBitSet> list = new ObjectArrayList<>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }

    /**
     * Get all combinations, which can be built out of the elements of a prefix
     * block
     *
     * @param list: List of OpenBitSets, which are in the same prefix block.
     * @return All combinations of the OpenBitSets.
     */
    private ObjectArrayList<OpenBitSet[]> getListCombinations(ObjectArrayList<OpenBitSet> list) {
        ObjectArrayList<OpenBitSet[]> combinations = new ObjectArrayList<>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                OpenBitSet[] combi = new OpenBitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }

    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the
     * last level. Only if this check return true X is added to the new level.
     */
    private boolean checkSubsets(OpenBitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        OpenBitSet Xclone = X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }

    private void generateNextLevel() {
        level0 = level1;
        level1 = null;
        System.gc();

        Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<>();

        buildPrefixBlocks();

        for (ObjectArrayList<OpenBitSet> prefix_block_list : prefix_blocks.values()) {

            // continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ObjectArrayList<OpenBitSet[]> combinations = getListCombinations(prefix_block_list);
            for (OpenBitSet[] c : combinations) {
                OpenBitSet X = c[0].clone();
                X.or(c[1]);

                if (checkSubsets(X)) {
                    StrippedPartition st = null;
                    CombinationHelper ch = new CombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                        st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
                    } else {
                        ch.setInvalid();
                    }
                    OpenBitSet rhsCandidates = new OpenBitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);

                    new_level.put(X, ch);
                }
            }
        }

        level1 = new_level;
    }

    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A OpenBitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     */
    private void addDependencyToResultReceiver(OpenBitSet X, int a) {
        if (this.fdResultReceiver == null) {
            return;
        }
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) X.cardinality()];
        int j = 0;
        for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
            columns[j++] = this.columnIdentifiers.get(i - 1);
        }
        ColumnCombination colCombination = new ColumnCombination(columns);
        FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get(a - 1));
        this.fdResultReceiver.receiveResult(fdResult);
    }

    private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<>(this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, column_name));
        }
    }

}
