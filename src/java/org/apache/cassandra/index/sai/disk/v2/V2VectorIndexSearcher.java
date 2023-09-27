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
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;
import org.apache.cassandra.index.sai.disk.v2.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OptimizeFor;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.tracing.Tracing;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

/**
 * Executes ann search against the graph for an individual index segment.
 */
public class V2VectorIndexSearcher extends IndexSearcher implements SegmentOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final JVectorLuceneOnDiskGraph graph;
    private final PrimaryKey.Factory keyFactory;
    private final SSTableId<?> sstableId;
    private int globalBruteForceRows; // not final so test can inject its own setting
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;

    public V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                 PerIndexFiles perIndexFiles,
                                 SegmentMetadata segmentMetadata,
                                 IndexDescriptor indexDescriptor,
                                 IndexContext indexContext) throws IOException
    {
        this(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext, new CassandraOnDiskHnsw(segmentMetadata.componentMetadatas, perIndexFiles, indexContext));
    }

    protected V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexDescriptor indexDescriptor,
                                    IndexContext indexContext,
                                    JVectorLuceneOnDiskGraph graph) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);
        this.graph = graph;
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));
        this.sstableId = primaryKeyMapFactory.getSSTableId();

        globalBruteForceRows = Integer.MAX_VALUE;
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public RangeIterator<Long> search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList results = searchPosting(context, exp, keyRange, limit);
        return toSSTableRowIdsIterator(results, context);
    }

    private PostingList searchPosting(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        int topK = topKFor(limit);
        BitsOrPostingList bitsOrPostingList = bitsOrPostingListForKeyRange(context, keyRange, topK);
        if (bitsOrPostingList.skipANN())
            return bitsOrPostingList.postingList();

        float[] queryVector = exp.lower.value.vector;
        var vectorPostings = graph.search(queryVector, topK, limit, bitsOrPostingList.getBits(), context,
                                          context.getScoreRecorder(sstableId, metadata.segmentRowIdOffset));
        if (bitsOrPostingList.expectedNodesVisited >= 0)
            updateExpectedNodes(vectorPostings.getVisitedCount(), bitsOrPostingList.expectedNodesVisited);
        return vectorPostings;
    }

    /**
     * If we are optimizing for recall, ask the index to search for more than `limit` results,
     * which (since it will search deeper in the graph) will tend to surface slightly better
     * candidates in the process.
     */
    private int topKFor(int limit)
    {
        // compute the factor `n` to multiply limit by to increase the number of results from the index.
        // "recall mode" scales smoothly from n=10 at limit=1 to n=2 at limit=100 to n=1 at limit=1000;
        // jvector testing shows that at limit 100 / n=2 the quantized results almost exactly match unquantized.
        var n = indexContext.getIndexWriterConfig().getOptimizeFor() == OptimizeFor.LATENCY
                ? 1
                : 0.271 + 9.729 * pow(limit, -0.375);
        return (int) (n * limit);
    }

    /**
     * Return bit set if needs to search HNSW; otherwise return posting list to bypass HNSW
     */
    private BitsOrPostingList bitsOrPostingListForKeyRange(QueryContext context, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());
            PrimaryKey lastPrimaryKey = keyFactory.createTokenOnly(keyRange.right.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.firstRowIdFromPrimaryKey(firstPrimaryKey);
            long maxSSTableRowId = primaryKeyMap.lastRowIdFromPrimaryKey(lastPrimaryKey);

            if (minSSTableRowId > maxSSTableRowId)
                return new BitsOrPostingList(PostingList.EMPTY);

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // If num of matches are not bigger than limit, skip ANN.
            // (nRows should not include shadowed rows, but context doesn't break those out by segment,
            // so we will live with the inaccuracy.)
            var nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            int maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(limit, nRows, graph.size()));
            logger.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                         nRows, maxBruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                          nRows, maxBruteForceRows, graph.size(), limit);
            if (nRows <= maxBruteForceRows)
            {
                IntArrayList postings = new IntArrayList(Math.toIntExact(nRows), -1);
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    if (context.shouldInclude(sstableRowId, primaryKeyMap))
                        postings.addInt(metadata.toSegmentRowId(sstableRowId));
                }
                return new BitsOrPostingList(new ArrayPostingList(postings.toIntArray()));
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            boolean hasMatches = false;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    if (context.shouldInclude(sstableRowId, primaryKeyMap))
                    {
                        int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                        int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                        if (ordinal >= 0)
                        {
                            bits.set(ordinal);
                            hasMatches = true;
                        }
                    }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (!hasMatches)
                return new BitsOrPostingList(PostingList.EMPTY);

            return new BitsOrPostingList(bits, VectorMemtableIndex.expectedNodesVisited(limit, nRows, graph.size()));
        }
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodes = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
        // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
        // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
        // so I'm leaving it as a 1:1 ratio for now.
        return max(limit, expectedNodes);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        var observedRatio = actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
        return (int) (observedRatio * VectorMemtableIndex.expectedNodesVisited(limit, nPermittedOrdinals, graphSize));
    }

    private void updateExpectedNodes(int actualNodesVisited, int expectedNodesVisited)
    {
        assert expectedNodesVisited >= 0 : expectedNodesVisited;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        if (actualNodesVisited >= 1000 && actualNodesVisited > 2 * expectedNodesVisited || expectedNodesVisited > 2 * actualNodesVisited)
            logger.warn("Predicted visiting {} nodes, but actually visited {}", expectedNodesVisited, actualNodesVisited);
        actualExpectedRatio.updateAndGet(actualNodesVisited, expectedNodesVisited);
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        var bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    @Override
    public RangeIterator<PrimaryKey> limitToTopResults(QueryContext context, RangeIterator<Long> iterator, Expression exp, int limit) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // the iterator represents keys from all the segments in our sstable -- we'll only pull of those that
            // are from our own token range so we can use row ids to order the results by vector similarity.
            var maxSegmentRowId = metadata.toSegmentRowId(metadata.maxSSTableRowId);
            SparseFixedBitSet bits = bitSetForSearch();
            var rowIds = new IntArrayList();
            try (var ordinalsView = graph.getOrdinalsView())
            {
                while (iterator.hasNext())
                {
                    Long sstableRowId = iterator.peek();
                    // if sstable row id has exceeded current ANN segment, stop
                    if (sstableRowId > metadata.maxSSTableRowId)
                        break;

                    iterator.next();
                    // skip rows that are not in our segment (or more preciesely, have no vectors that were indexed)
                    if (sstableRowId < metadata.minSSTableRowId)
                        continue;

                    int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                    rowIds.add(segmentRowId);

                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                    {
                        if (context.shouldInclude(sstableRowId, primaryKeyMap))
                            bits.set(ordinal);
                    }
                }
            }

            // if we have a small number of results then let TopK processor do exact NN computation
            int topK = topKFor(limit);
            var maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(topK, rowIds.size(), graph.size()));
            logger.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes of degree {}, LIMIT {}",
                         rowIds.size(), maxBruteForceRows, graph.size(), indexContext.getIndexWriterConfig().getMaximumNodeConnections(), limit);
            Tracing.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes of degree {}, LIMIT {}",
                          rowIds.size(), maxBruteForceRows, graph.size(), indexContext.getIndexWriterConfig().getMaximumNodeConnections(), limit);
            if (rowIds.size() <= maxBruteForceRows)
                return toPrimaryKeyIterator(new ArrayPostingList(rowIds.toIntArray()), context, true);

            // else ask the index to perform a search limited to the bits we created
            float[] queryVector = exp.lower.value.vector;
            var results = graph.search(queryVector, topK, limit, bits, context,
                                       context.getScoreRecorder(sstableId, metadata.segmentRowIdOffset));
            updateExpectedNodes(results.getVisitedCount(), expectedNodesVisited(topK, maxSegmentRowId, graph.size()));
            return toPrimaryKeyIterator(results, context, false);
        }
    }

    RangeIterator<PrimaryKey> toPrimaryKeyIterator(PostingList postingList, QueryContext queryContext, boolean isBruteForce) throws IOException
    {
        if (postingList == null || postingList.size() == 0)
            return RangeIterator.emptyKeys();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        postingList.peekable());

        return new PostingListRangeIterator(indexContext, primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(), searcherContext, isBruteForce);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }

    private static class BitsOrPostingList
    {
        private final Bits bits;
        private final int expectedNodesVisited;
        private final PostingList postingList;

        public BitsOrPostingList(@Nullable Bits bits, int expectedNodesVisited)
        {
            this.bits = bits;
            this.expectedNodesVisited = expectedNodesVisited;
            this.postingList = null;
        }

        public BitsOrPostingList(@Nullable Bits bits)
        {
            this.bits = bits;
            this.postingList = null;
            this.expectedNodesVisited = -1;
        }

        public BitsOrPostingList(PostingList postingList)
        {
            this.bits = null;
            this.postingList = Preconditions.checkNotNull(postingList);
            this.expectedNodesVisited = -1;
        }

        @Nullable
        public Bits getBits()
        {
            Preconditions.checkState(!skipANN());
            return bits;
        }

        public PostingList postingList()
        {
            Preconditions.checkState(skipANN());
            return postingList;
        }

        public boolean skipANN()
        {
            return postingList != null;
        }
    }
}
