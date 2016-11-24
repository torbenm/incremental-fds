package org.mp.naumann.algorithms.fd.structures.plis;

import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.mp.naumann.database.TableInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class PliCollection implements Collection<PositionListIndex> {

    private static final Logger LOG = Logger.getLogger(PliCollection.class.getName());

    private List<PositionListIndex> plis = new ArrayList<>();
    private final int numLastRecords;
    private final int numAttributes;
    int[][] inverted;
    int[][] compressed;

    public PliCollection(List<PositionListIndex> plis, int numAttributes, int numLastRecords) {
        this.plis = plis;
        this.numAttributes = numAttributes;
        this.numLastRecords = numLastRecords;
    }

    public int[][] getInverted(){
        if(inverted == null)
            inverted = invert();
        return inverted;
    }

    public int[][] getCompressed(){
        if(compressed == null)
            compressed = compress();
        return compressed;
    }

    public int getNumberOfLastRecords() {
        return numLastRecords;
    }

    public int getNumberOfAttributes() {
        return numAttributes;
    }

    public PositionListIndex get(int index){
        return plis.get(index);
    }

    public void sortByNumOfClusters(){
        LOG.info("Sorting plis by cluster...");
        Collections.sort(plis, (o1, o2) -> {
            int numClustersInO1 = numLastRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
            int numClustersInO2 = numLastRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
            return numClustersInO2 - numClustersInO1;
        });
    }


    private int[][] invert() {
        LOG.info("Inverting plis ...");
        int[][] invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numLastRecords];
            Arrays.fill(invertedPli, -1);

            for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
                for (int recordId : plis.get(attr).getClusters().get(clusterId))
                    invertedPli[recordId] = clusterId;
            }
            invertedPlis[attr] = invertedPli;
        }
        return invertedPlis;
    }

    private int[][] compress(){
        LOG.info("Extracting integer representations for the records ...");
        int[][] compressedRecords = new int[numLastRecords][];
        for (int recordId = 0; recordId < numLastRecords; recordId++)
            compressedRecords[recordId] = fetchRecordFrom(recordId, getInverted());
        return compressedRecords;
    }

    private int[] fetchRecordFrom(int recordId, int[][] plis) {
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++)
            record[i] = plis[i][recordId];
        return record;
    }

    public static PliCollection readFromTableInput(TableInput tableInput, int numAttributes, ValueComparator valueComparator){
        PliBuilder pliBuilder = new PliBuilder();
        List<PositionListIndex> plis =pliBuilder.getPlis(tableInput, numAttributes,
                valueComparator.isNullEqualNull());
        return new PliCollection(plis, numAttributes, pliBuilder.getNumLastRecords());
    }

    @Override
    public int size() {
        return plis.size();
    }

    @Override
    public boolean isEmpty() {
        return plis.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return plis.contains(o);
    }

    @Override
    public Iterator<PositionListIndex> iterator() {
        return plis.iterator();
    }

    @Override
    public Object[] toArray() {
        return plis.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return plis.toArray(a);
    }

    @Override
    public boolean add(PositionListIndex positionListIndex) {
        return plis.add(positionListIndex);
    }

    @Override
    public boolean remove(Object o) {
        return plis.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return plis.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends PositionListIndex> c) {
        return plis.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return plis.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return plis.retainAll(c);
    }

    @Override
    public void clear() {
        plis.clear();
    }


}
