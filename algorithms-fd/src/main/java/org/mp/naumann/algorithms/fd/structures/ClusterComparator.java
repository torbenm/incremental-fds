package org.mp.naumann.algorithms.fd.structures;

import java.util.Comparator;

public class ClusterComparator implements Comparator<Integer> {

    private int[][] sortKeys;
    private int activeKey1;
    private int activeKey2;

    public ClusterComparator(int[][] sortKeys, int activeKey1, int activeKey2) {
        super();
        this.sortKeys = sortKeys;
        this.activeKey1 = activeKey1;
        this.activeKey2 = activeKey2;
    }

    public void incrementActiveKey() {
        this.activeKey1 = this.increment(this.activeKey1);
        this.activeKey2 = this.increment(this.activeKey2);
    }

    @Override
    public int compare(Integer o1, Integer o2) {
        int value1 = this.sortKeys[o1][this.activeKey1];
        int value2 = this.sortKeys[o2][this.activeKey1];
        int result = value2 - value1;
        if (result == 0) {
            value1 = this.sortKeys[o1][this.activeKey2];
            value2 = this.sortKeys[o2][this.activeKey2];
            result = value2 - value1;
        }
        return result;
    }

    private int increment(int number) {
        return (number == this.sortKeys[0].length - 1) ? 0 : number + 1;
    }
}