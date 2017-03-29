package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class OpenBitSetFD {
    private OpenBitSet lhs;
    private int rhs;

    public OpenBitSetFD(OpenBitSet lhs, int rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public OpenBitSet getLhs() {
        return lhs;
    }

    public int getRhs() {
        return rhs;
    }

    @Override
    public String toString() {
        return BitSetUtils.toString(lhs) + "->" + rhs;
    }

    public String toString(int lhsSize) {
        return BitSetUtils.toString(lhs, lhsSize) + "->" + rhs;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof OpenBitSetFD)) {
            return false;
        }

        OpenBitSetFD other = (OpenBitSetFD) o;

        return other.lhs.equals(lhs) &&
                other.rhs == rhs;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + lhs.hashCode();
        result = 31 * result + Integer.hashCode(rhs);
        return result;
    }
}
