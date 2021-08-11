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

package org.apache.cassandra.index.sai;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.Segment;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexComponents.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;


/**
 * SSTableIndex is created for each column index on individual sstable to track per-column indexer.
 */
public class SSTableIndex
{
    // sort sstable index by first key then last key
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.generation);

    private final Version version;
    private final SSTableContext sstableContext;
    private final ColumnContext columnContext;
    public final SSTableReader sstable;
    public final IndexComponents components;

    public final Segment segment;
    private PerIndexFiles indexFiles;

    private final SegmentMetadata metadata;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, ColumnContext columnContext, IndexComponents components)
    {
        this.sstableContext = sstableContext.sharedCopy();
        this.columnContext = columnContext;
        this.sstable = sstableContext.sstable;
        this.components = components;

        final AbstractType<?> validator = columnContext.getValidator();
        assert validator != null;

        try
        {
            this.indexFiles = new PerIndexFiles(components, columnContext.isLiteral());

            final MetadataSource source = MetadataSource.loadColumnMetadata(components);
            version = source.getVersion();

            //TODO Need a compressor here
            metadata = SegmentMetadata.load(source, columnContext.keyFactory(), null);
            segment = new Segment(columnContext, this.sstableContext, indexFiles, metadata);

        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    public ColumnContext getColumnContext()
    {
        return columnContext;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public long indexFileCacheSize()
    {
        return segment.indexFileCacheSize();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return metadata.numRows;
    }

    /**
     * @return total size of per-column index components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return components.sizeOfPerColumnComponents();
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return metadata.minSSTableRowId;
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return metadata.maxSSTableRowId;
    }

    public ByteBuffer minTerm()
    {
        return metadata.minTerm;
    }

    public ByteBuffer maxTerm()
    {
        return metadata.maxTerm;
    }

    public PrimaryKey minKey()
    {
        return metadata.minKey;
    }

    public PrimaryKey maxKey()
    {
        return metadata.maxKey;
    }

    public List<RangeIterator> search(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context)
    {
        return segment.intersects(keyRange) ? segment.search(expression, context) : Collections.EMPTY_LIST;
    }

    public PostingList searchPostingList(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context)
    {
        return segment.intersects(keyRange) ? segment.searchPostingList(expression, context) : null;
    }

    public SegmentMetadata segment()
    {
        return metadata;
    }

    public Version getVersion()
    {
        return version;
    }

    /**
     * container to share per-index file handles(kdtree, terms data, posting lists) among segments.
     */
    public static class PerIndexFiles implements Closeable
    {
        private final Map<IndexComponent, FileHandle> files = new HashMap<>(2);
        private final IndexComponents components;

        public PerIndexFiles(IndexComponents components, boolean isStringIndex)
        {
            this(components, isStringIndex, false);
        }

        public PerIndexFiles(IndexComponents components, boolean isStringIndex, boolean temporary)
        {
            this.components = components;
            if (isStringIndex)
            {
                files.put(components.postingLists, components.createFileHandle(components.postingLists, temporary));
                files.put(components.termsData, components.createFileHandle(components.termsData, temporary));
            }
            else
            {
                files.put(components.kdTree, components.createFileHandle(components.kdTree, temporary));
                files.put(components.kdTreePostingLists, components.createFileHandle(components.kdTreePostingLists, temporary));
            }
        }

        public FileHandle kdtree()
        {
            return getFile(components.kdTree);
        }

        public FileHandle postingLists()
        {
            return getFile(components.postingLists);
        }

        public FileHandle termsData()
        {
            return getFile(components.termsData);
        }

        public FileHandle kdtreePostingLists()
        {
            return getFile(components.kdTreePostingLists);
        }

        private FileHandle getFile(IndexComponent type)
        {
            FileHandle file = files.get(type);
            if (file == null)
                throw new IllegalArgumentException(String.format("Component %s not found for SSTable %s", type.name, components.descriptor));

            return file;
        }

        public IndexComponents components()
        {
            return this.components;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(files.values());
        }
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public boolean isReleased()
    {
        return references.get() <= 0;
    }

    public void release()
    {
        int n = references.decrementAndGet();

        if (n == 0)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(segment);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild.
             */
            if (obsolete.get())
            {
                components.deleteColumnIndex();
            }
        }
    }

    public void markObsolete()
    {
        obsolete.getAndSet(true);
        release();
    }

    public boolean equals(Object o)
    {
        return o instanceof SSTableIndex && components.equals(((SSTableIndex) o).components);
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(components.hashCode()).build();
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", columnContext.getColumnName())
                          .add("sstable", sstable.descriptor)
                          .add("totalRows", sstable.getTotalRows())
                          .toString();
    }
}
