/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class View implements Iterable<SSTableIndex>
{
    private final Map<Descriptor, SSTableIndex> view;

    private final TermTree termTree;
    private final AbstractType<?> keyValidator;
    private final IntervalTree<PrimaryKey, SSTableIndex, Interval<PrimaryKey, SSTableIndex>> keyIntervalTree;

    public View(IndexContext context, Collection<SSTableIndex> indexes)
    {
        this.view = new HashMap<>();
        this.keyValidator = context.keyValidator();

        AbstractType<?> validator = context.getValidator();

        TermTree.Builder termTreeBuilder = new RangeTermTree.Builder(validator);

        List<Interval<PrimaryKey, SSTableIndex>> keyIntervals = new ArrayList<>();
        for (SSTableIndex sstableIndex : indexes)
        {
            this.view.put(sstableIndex.getSSTable().descriptor, sstableIndex);
            termTreeBuilder.add(sstableIndex);
            keyIntervals.add(Interval.create(sstableIndex.minKey(),
                                             sstableIndex.maxKey(),
                                             sstableIndex));
        }

        this.termTree = termTreeBuilder.build();
        this.keyIntervalTree = IntervalTree.build(keyIntervals);
    }

    public Set<SSTableIndex> match(Expression expression)
    {
        return termTree.search(expression);
    }

    public List<SSTableIndex> match(PrimaryKey minKey, PrimaryKey maxKey)
    {
        return keyIntervalTree.search(Interval.create(minKey, maxKey, null));
    }

    public Iterator<SSTableIndex> iterator()
    {
        return view.values().iterator();
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    public boolean containsSSTable(SSTableReader sstable)
    {
        return view.containsKey(sstable.descriptor);
    }

    public int size()
    {
        return view.size();
    }

    @Override
    public String toString()
    {
        return String.format("View{view=%s, keyValidator=%s, keyIntervalTree=%s}", view, keyValidator, keyIntervalTree);
    }
}
