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

package org.apache.cassandra.index.sai.disk.v2.hnsw;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.db.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.tracing.Tracing;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class CassandraOnDiskHnsw extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnDiskHnsw.class);

    private final Function<QueryContext, VectorsWithCache> vectorsSupplier;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorCache vectorCache;

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;
    private FileHandle vectorsFile;

    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        vectorsFile = indexFiles.vectors();
        long vectorsSegmentOffset = getComponentMetadata(IndexComponent.VECTOR).offset;
        vectorsSupplier = (qc) -> new VectorsWithCache(new OnDiskVectors(vectorsFile, vectorsSegmentOffset), qc);

        SegmentMetadata.ComponentMetadata postingListsMetadata = getComponentMetadata(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = getComponentMetadata(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
        var mockContext = new QueryContext();
        try (var vectors = new OnDiskVectors(vectorsFile, vectorsSegmentOffset))
        {
            vectorCache = VectorCache.load(hnsw.getView(mockContext), vectors, CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getInt());
        }
    }

    @Override
    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes() + vectorCache.ramBytesUsed();
    }

    @Override
    public int size()
    {
        return hnsw.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    @Override
    public VectorPostingList search(float[] queryVector, int topK, int limit, Bits acceptBits, QueryContext context)
    {
        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        NeighborQueue queue;
        try (var vectors = vectorsSupplier.apply(context); var view = hnsw.getView(context))
        {
            final long start = System.nanoTime();
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectors,
                                             VectorEncoding.FLOAT32,
                                             LuceneCompat.vsf(similarityFunction),
                                             view,
                                             LuceneCompat.bits(ordinalsMap.ignoringDeleted(acceptBits)),
                                             Integer.MAX_VALUE);
            context.addDiskhnswSearches(queue.visitedCount(), queue.size());
            context.markDiskHnswLatencies(System.nanoTime() - start);
            return annRowIdsToPostings(queue);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public VectorPostingList search(float[] queryVector, int topK, float threshold, int limit, Bits bits, QueryContext context)
    {
        if (threshold > 0)
            throw new InvalidRequestException("Geo queries are not supported for legacy SAI indexes -- drop the index and recreate it to enable these");

        return search(queryVector, topK, limit, bits, context);
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final NeighborQueue queue;
        private final RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NeighborQueue queue)
        {
            this.queue = queue;
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && queue.size() > 0) {
                try
                {
                    var ordinal = queue.pop();
                    segmentRowIdIterator = Arrays.stream(rowIdsView.getSegmentRowIdsMatching(ordinal)).iterator();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return segmentRowIdIterator.hasNext();
        }

        @Override
        public int nextInt() {
            if (!hasNext())
                throw new NoSuchElementException();
            return segmentRowIdIterator.nextInt();
        }

        @Override
        public void close()
        {
            rowIdsView.close();
        }
    }

    private VectorPostingList annRowIdsToPostings(NeighborQueue queue) throws IOException
    {
        try (var iterator = new RowIdIterator(queue))
        {
            // JVector returns results ordered most- to least-similar, which is why VPL has a `limit` paramter
            // to avoid sorting results we don't care about.  But Lucene returns them with the least-similar
            // results at the front of the queue, so we ensure we exhaust the whole queue here.
            return new VectorPostingList(iterator, queue.size(), queue.visitedCount());
        }
    }

    @Override
    public void close()
    {
        vectorsFile.close();
        ordinalsMap.close();
        hnsw.close();
    }

    @Override
    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }

    @NotThreadSafe
    class VectorsWithCache implements RandomAccessVectorValues<float[]>, AutoCloseable
    {
        private final OnDiskVectors vectors;
        private final QueryContext queryContext;

        public VectorsWithCache(OnDiskVectors vectors, QueryContext queryContext)
        {
            this.vectors = vectors;
            this.queryContext = queryContext;
        }

        @Override
        public int size()
        {
            return vectors.size();
        }

        @Override
        public int dimension()
        {
            return vectors.dimension();
        }

        @Override
        public float[] vectorValue(int i) throws IOException
        {
            queryContext.addHnswVectorsAccessed(1);
            var cached = vectorCache.get(i);
            if (cached != null)
            {
                queryContext.addHnswVectorCacheHits(1);
                return cached;
            }

            return vectors.vectorValue(i);
        }

        @Override
        public RandomAccessVectorValues<float[]> copy()
        {
            throw new UnsupportedOperationException();
        }

        public void close()
        {
            vectors.close();
        }
    }
}
