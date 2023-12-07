/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Triple;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.utils.Pair;

public class SaiTopKProcessor
{

    private final ReadCommand command;
    private final int limit;

    public SaiTopKProcessor(ReadCommand command)
    {
        this.command = command;
        this.limit = command.limits().count();
//command.indexQueryPlan().getIndexes().stream().findFirst().get().
    }

    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> filter(P partitions)
    {
        PriorityQueue<Pair<PartitionInfo, Row>> topK = null;
        //new PriorityQueue<>(limit, Comparator.comparing((Pair<PartitionInfo, Row> t) -> t.right().getCell()).reversed());

        TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));

        while (partitions.hasNext())
        {
            // have to close to move to the next partition, otherwise hasNext() fails
            try (var partitionRowIterator = partitions.next())
            {
                PartitionResults pr = processPartition(partitionRowIterator, limit);
                topK.addAll(pr.rows);
                for (var uf: pr.tombstones)
                    addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
            }
        }


        return null;
    }

    private class PartitionResults {
        final PartitionInfo partitionInfo;
        final SortedSet<Unfiltered> tombstones = new TreeSet<>(command.metadata().comparator);
        final List<Pair<PartitionInfo, Row>> rows = new ArrayList<>();

        PartitionResults(PartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        void addTombstone(Unfiltered uf)
        {
            tombstones.add(uf);
        }

        void addRow(Pair<PartitionInfo, Row> tuple) {
            rows.add(tuple);
        }
    }

    private PartitionResults processPartition(BaseRowIterator<?> partitionRowIterator, final int limit) {
        // Compute key and static row score once per partition
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        var pr = new PartitionResults(partitionInfo);

        int found = 0;
        while (partitionRowIterator.hasNext() && found < limit)
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
            // TombstoneOverwhelmingException to prevent OOM.
            if (unfiltered.isRangeTombstoneMarker())
            {
                pr.addTombstone(unfiltered);
                continue;
            }

            Row row = (Row) unfiltered;
            pr.addRow(Pair.create(partitionInfo, row));
            found++;
        }

        return pr;
    }

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition, PartitionInfo partitionInfo, Unfiltered unfiltered)
    {
        var map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        map.add(unfiltered);
    }

}
