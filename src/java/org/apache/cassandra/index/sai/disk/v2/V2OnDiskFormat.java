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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RowAwarePrimaryKeyFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class V2OnDiskFormat extends V1OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Set<IndexComponent> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                 IndexComponent.GROUP_META,
                                                                                 IndexComponent.TOKEN_VALUES,
                                                                                 IndexComponent.TRIE_DATA,
                                                                                 IndexComponent.SORTED_BYTES,
                                                                                 IndexComponent.BLOCK_POINTERS);

    public static final V2OnDiskFormat instance = new V2OnDiskFormat();

    private static final IndexFeatureSet v2IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }
    };

    protected V2OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v2IndexFeatureSet;
    }

    @Override
    public PrimaryKey.PrimaryKeyFactory primaryKeyFactory(IPartitioner partitioner, ClusteringComparator comparator)
    {
        return new RowAwarePrimaryKeyFactory(partitioner, comparator);
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException
    {
        return new V2PrimaryKeyMap.V2PrimaryKeyMapFactory(indexDescriptor, sstable);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new SSTableComponentsWriter(indexDescriptor);
    }

    @Override
    public Set<IndexComponent> perSSTableComponents()
    {
        return PER_SSTABLE_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 4;
    }

}
