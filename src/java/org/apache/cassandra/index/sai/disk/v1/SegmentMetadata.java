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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Multiple {@link SegmentMetadata} are stored in {@link IndexComponent#META} file, each corresponds to an on-disk
 * index segment.
 */
public class SegmentMetadata implements Comparable<SegmentMetadata>
{
    private static final String NAME = "SegmentMetadata";

    /**
     * Used to retrieve sstableRowId which equals to offset plus segmentRowId.
     */
    public final long segmentRowIdOffset;

    /**
     * Min and max sstable rowId in current segment.
     *
     * For index generated by compaction, minSSTableRowId is the same as segmentRowIdOffset.
     * But for flush, segmentRowIdOffset is taken from previous segment's maxSSTableRowId.
     */
    public final long minSSTableRowId;
    public final long maxSSTableRowId;

    /**
     * number of indexed rows (aka. pair of term and segmentRowId) in current segment
     */
    public final long numRows;

    /**
     * Ordered by their token position in current segment
     */
    public final PrimaryKey minKey;
    public final PrimaryKey maxKey;

    /**
     * Minimum and maximum indexed column value ordered by its {@link org.apache.cassandra.db.marshal.AbstractType}.
     */
    public final ByteBuffer minTerm, maxTerm;

    /**
     * Root, offset, length for each index structure in the segment.
     *
     * Note: postings block offsets are stored in terms dictionary, no need to worry about its root.
     */
    public final ComponentMetadataMap componentMetadatas;

    public SegmentMetadata(long segmentRowIdOffset,
                           long numRows,
                           long minSSTableRowId,
                           long maxSSTableRowId,
                           PrimaryKey minKey,
                           PrimaryKey maxKey,
                           ByteBuffer minTerm,
                           ByteBuffer maxTerm,
                           ComponentMetadataMap componentMetadatas)
    {
        assert numRows < Integer.MAX_VALUE;
        Objects.requireNonNull(minKey);
        Objects.requireNonNull(maxKey);
        Objects.requireNonNull(minTerm);
        Objects.requireNonNull(maxTerm);

        this.segmentRowIdOffset = segmentRowIdOffset;
        this.minSSTableRowId = minSSTableRowId;
        this.maxSSTableRowId = maxSSTableRowId;
        this.numRows = numRows;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.componentMetadatas = componentMetadatas;
    }

    private static final Logger logger = LoggerFactory.getLogger(SegmentMetadata.class);

    @Override
    public int compareTo(SegmentMetadata other)
    {
        return Long.compare(this.segmentRowIdOffset, other.segmentRowIdOffset);
    }

    @Override
    public String toString()
    {
        return "SegmentMetadata{" +
               "segmentRowIdOffset=" + segmentRowIdOffset +
               ", minSSTableRowId=" + minSSTableRowId +
               ", maxSSTableRowId=" + maxSSTableRowId +
               ", numRows=" + numRows +
               ", componentMetadatas=" + componentMetadatas +
               '}';
    }

    public long getIndexRoot(IndexComponent indexComponent)
    {
        return componentMetadatas.get(indexComponent).root;
    }

    public long getIndexOffset(IndexComponent indexComponent)
    {
        return componentMetadatas.get(indexComponent).offset;
    }

    public long getIndexLength(IndexComponent indexComponent)
    {
        return componentMetadatas.get(indexComponent).length;
    }

    public static class ComponentMetadataMap
    {
        private final Map<IndexComponent, ComponentMetadata> metas = new HashMap<>();

        public ComponentMetadataMap(IndexInput input) throws IOException
        {
            int size = input.readInt();

            for (int i = 0; i < size; i++)
            {
                metas.put(IndexComponent.valueOf(input.readString()), new ComponentMetadata(input));
            }
        }

        public ComponentMetadataMap(DataInputPlus input) throws IOException
        {
            int size = input.readInt();

            for (int i = 0; i < size; i++)
            {
                metas.put(IndexComponent.valueOf(input.readUTF()), new ComponentMetadata(input));
            }
        }

        public ComponentMetadataMap()
        {
        }

        public void put(IndexComponent indexComponent, long root, long offset, long length)
        {
            metas.put(indexComponent, new ComponentMetadata(root, offset, length));
        }

        public void put(IndexComponent indexComponent, long root, long offset, long length, Map<String, String> additionalMap)
        {
            metas.put(indexComponent, new ComponentMetadata(root, offset, length, additionalMap));
        }

        public void write(IndexOutput output) throws IOException
        {
            output.writeInt(metas.size());

            for (Map.Entry<IndexComponent, ComponentMetadata> entry : metas.entrySet())
            {
                output.writeString(entry.getKey().name());
                entry.getValue().write(output);
            }
        }

        public void write(DataOutputPlus output) throws IOException
        {
            output.writeInt(metas.size());

            for (Map.Entry<IndexComponent, ComponentMetadata> entry : metas.entrySet())
            {
                output.writeUTF(entry.getKey().name());
                entry.getValue().write(output);
            }
        }

        public ComponentMetadata get(IndexComponent indexComponent)
        {
            if (!metas.containsKey(indexComponent))
                throw new IllegalArgumentException(indexComponent + " ComponentMetadata not found");

            return metas.get(indexComponent);
        }

        public Map<String, Map<String, String>> asMap()
        {
            Map<String, Map<String, String>> metaAttributes = new HashMap<>();

            for (Map.Entry<IndexComponent, ComponentMetadata> entry : metas.entrySet())
            {
                String name = entry.getKey().name();
                ComponentMetadata metadata = entry.getValue();

                Map<String, String> componentAttributes = metadata.asMap();

                assert !metaAttributes.containsKey(name) : "Found duplicate index type: " + name;
                metaAttributes.put(name, componentAttributes);
            }

            return metaAttributes;
        }

        @Override
        public String toString()
        {
            return "ComponentMetadataMap{" +
                   "metas=" + metas +
                   '}';
        }

        public double indexSize()
        {
            return metas.values().stream().mapToLong(meta -> meta.length).sum();
        }
    }

    public static class ComponentMetadata
    {
        public static final String ROOT = "Root";
        public static final String OFFSET = "Offset";
        public static final String LENGTH = "Length";

        public final long root;
        public final long offset;
        public final long length;
        public final Map<String,String> attributes;

        ComponentMetadata(long root, long offset, long length)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = Collections.emptyMap();
        }

        ComponentMetadata(long root, long offset, long length, Map<String, String> attributes)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = attributes;
        }

        ComponentMetadata(IndexInput input) throws IOException
        {
            this.root = input.readLong();
            this.offset = input.readLong();
            this.length = input.readLong();
            int size = input.readInt();

            attributes = new HashMap<>(size);
            for (int x=0; x < size; x++)
            {
                String key = input.readString();
                String value = input.readString();

                attributes.put(key, value);
            }
        }

        ComponentMetadata(DataInputPlus input) throws IOException
        {
            this.root = input.readLong();
            this.offset = input.readLong();
            this.length = input.readLong();
            int size = input.readInt();

            attributes = new HashMap<>(size);
            for (int x=0; x < size; x++)
            {
                String key = input.readUTF();
                String value = input.readUTF();

                attributes.put(key, value);
            }
        }

        public void write(IndexOutput output) throws IOException
        {
            output.writeLong(root);
            output.writeLong(offset);
            output.writeLong(length);

            output.writeInt(attributes.size());
            for (Map.Entry<String,String> entry : attributes.entrySet())
            {
                output.writeString(entry.getKey());
                output.writeString(entry.getValue());
            }
        }

        public void write(DataOutputPlus output) throws IOException
        {
            output.writeLong(root);
            output.writeLong(offset);
            output.writeLong(length);

            output.writeInt(attributes.size());
            for (Map.Entry<String,String> entry : attributes.entrySet())
            {
                output.writeUTF(entry.getKey());
                output.writeUTF(entry.getValue());
            }
        }

        @Override
        public String toString()
        {
            return String.format("ComponentMetadata{root=%d, offset=%d, length=%d, attributes=%s}", root, offset, length, attributes.toString());
        }

        public Map<String, String> asMap()
        {
            return ImmutableMap.<String, String>builder().putAll(attributes).put(OFFSET, Long.toString(offset)).put(LENGTH, Long.toString(length)).put(ROOT, Long.toString(root)).build();
        }
    }
}