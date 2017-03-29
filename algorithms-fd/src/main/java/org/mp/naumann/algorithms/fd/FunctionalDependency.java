package org.mp.naumann.algorithms.fd;
/*
 * Copyright 2014 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;

/**
 * Represents a functional dependency.
 *
 * @author Jakob Zwiener
 */
public class FunctionalDependency {

    private static final String FD_SEPARATOR = "->";

    private ColumnCombination determinant;
    private ColumnIdentifier dependant;

    public FunctionalDependency(ColumnCombination determinant, ColumnIdentifier dependant) {
        this.determinant = determinant;
        this.dependant = dependant;
    }

    @Override
    public String toString() {
        return determinant.toString() + FD_SEPARATOR + dependant.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dependant == null) ? 0 : dependant.hashCode());
        result = prime * result + ((determinant == null) ? 0 : determinant.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FunctionalDependency other = (FunctionalDependency) obj;
        if (dependant == null) {
            if (other.dependant != null) {
                return false;
            }
        } else if (!dependant.equals(other.dependant)) {
            return false;
        }
        if (determinant == null) {
            if (other.determinant != null) {
                return false;
            }
        } else if (!determinant.equals(other.determinant)) {
            return false;
        }
        return true;
    }

}
