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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMerger;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.TermsIteratorMerger;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;

public class V3SegmentMerger implements SegmentMerger
{
    private ByteBuffer minTerm = null, maxTerm = null;
    private long maxRowid = Long.MIN_VALUE;
    private long minRowId = Long.MAX_VALUE;
    //private final List<BlockTerms.Reader.PointsIterator> segmentIterators = new ArrayList<>();
    private final List<TermsIterator> segmentTermsIterators = new ArrayList<>();

    private final List<TermsReader> readers = new ArrayList<>();

    @Override
    public void addSegment(IndexContext context, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        if (!(indexFiles instanceof V3PerIndexFiles))
            throw new IllegalArgumentException();

        minTerm = TypeUtil.min(segment.minTerm, minTerm, context.getValidator());
        maxTerm = TypeUtil.max(segment.maxTerm, maxTerm, context.getValidator());
        minRowId = Math.min(segment.minSSTableRowId, minRowId);
        maxRowid = Math.max(segment.maxSSTableRowId, maxRowid);

        segmentTermsIterators.add(createTermsIterator(context, segment, (V3PerIndexFiles) indexFiles));
    }

//    private BlockTerms.Reader.PointsIterator createIterator(IndexContext indexContext,
//                                                            SegmentMetadata segment,
//                                                            V3PerIndexFiles indexFiles) throws IOException
//    {
//        final BlockTerms.Reader reader = new BlockTerms.Reader(indexFiles.indexDescriptor(),
//                                                               indexContext,
//                                                               indexFiles,
//                                                               segment.componentMetadatas);
//        return reader.pointsIterator(reader, segment.segmentRowIdOffset);
//    }

    @Override
    public boolean isEmpty()
    {
        return segmentTermsIterators.isEmpty();
    }

    @Override
    public SegmentMetadata merge(IndexDescriptor indexDescriptor,
                                 IndexContext indexContext,
                                 PrimaryKey minKey,
                                 PrimaryKey maxKey,
                                 long maxSSTableRowId) throws IOException
    {
        try
        {
            try (final TermsIteratorMerger merger = new TermsIteratorMerger(segmentTermsIterators.toArray(new TermsIterator[0]), indexContext.getValidator()))
            {
                V3IndexWriter writer = new V3IndexWriter(indexDescriptor, indexContext, false);
                SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(merger);

                close();

                return new SegmentMetadata(0,
                                           writer.getPostingsAdded(),
                                           merger.minSSTableRowId,
                                           merger.maxSSTableRowId,
                                           minKey,
                                           maxKey,
                                           merger.getMinTerm(),
                                           merger.getMaxTerm(),
                                           indexMetas);
            }
        }
        catch (Exception ex)
        {
             throw new IOException(ex);
        }
    }

    @Override
    public void close() throws IOException
    {
        readers.forEach(TermsReader::close);
    }

    private TermsIterator createTermsIterator(IndexContext indexContext, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        final long root = segment.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        final Map<String, String> map = segment.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        final String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        final long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        final TermsReader termsReader = new TermsReader(indexContext,
                                                        indexFiles.termsData().sharedCopy(),
                                                        indexFiles.postingLists().sharedCopy(),
                                                        root,
                                                        footerPointer);
        readers.add(termsReader);
        return termsReader.allTerms(segment.segmentRowIdOffset);
    }

//    @Override
//    public void close() throws IOException
//    {
//        for (BlockTerms.Reader.PointsIterator iterator : segmentIterators)
//            FileUtils.closeQuietly(iterator);
//    }
}
