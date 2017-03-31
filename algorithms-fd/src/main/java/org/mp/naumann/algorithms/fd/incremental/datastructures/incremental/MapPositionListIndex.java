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

package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

/**
 * Position list indices (or stripped partitions) are an index structure that
 * stores the positions of equal values in a nested list. A column with the
 * values a, a, b, c, b, c transfers to the position list index ((0, 1), (2, 4),
 * (3, 5)). Clusters of size 1 are discarded. A position list index should be
 * created using the {@link PLIBuilder}.
 */
class MapPositionListIndex extends PositionListIndex {

    private final Map<Integer, Collection<Integer>> clusters;

    @Override
    public Collection<? extends Collection<Integer>> getClusters() {
        return this.clusters.entrySet().stream().filter(e -> !e.getKey().equals(PliUtils.UNIQUE_VALUE)).map(Entry::getValue).collect(Collectors.toList());
    }

    @Override
    public Collection<Integer> getCluster(int index) {
        return clusters.get(index);
    }

    MapPositionListIndex(int attribute, Map<Integer, Collection<Integer>> clusters) {
        super(attribute);
        this.clusters = clusters;
    }

    Map<Integer, Collection<Integer>> getRawClusters() {
        return clusters;
    }
}
