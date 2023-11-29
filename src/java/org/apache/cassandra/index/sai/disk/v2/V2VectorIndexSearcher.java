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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Reservoir;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.ListRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.metrics.SlidingWindowReservoirWithQuickMean;
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

    private static class ReservoirBucket
    {
        private final int[] limits;
        private final SlidingWindowReservoirWithQuickMean[] reservoirs;

        public ReservoirBucket(int[] limits, int reservoirSize)
        {
            this.limits = limits;
            Arrays.sort(limits);

            this.reservoirs = new SlidingWindowReservoirWithQuickMean[limits.length];
            for (int i = 0; i < limits.length; i++)
                this.reservoirs[i] = new SlidingWindowReservoirWithQuickMean(reservoirSize);
        }

        public SlidingWindowReservoirWithQuickMean get(int limit)
        {
            int index = findClosestIndex(this.limits, limit);
            assert index >= 0;
            assert index < reservoirs.length;
            return reservoirs[index];
        }

        private static int findClosestIndex(int[] limits, int target)
        {
            int left = 0;
            int right = limits.length;

            while (left < right)
            {
                int mid = (left + right) / 2;
                if (limits[mid] == target)
                    return mid;

                if (limits[mid] < target)
                    left = mid + 1;
                else
                    right = mid;
            }
            return left;
        }
    }

    private final JVectorLuceneOnDiskGraph graph;
    private final PrimaryKey.Factory keyFactory;
    private int globalBruteForceRows; // not final so test can inject its own setting
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ReservoirBucket bruteForceRowsReservoir = new ReservoirBucket(new int[] {10, 100, 250, 500, Integer.MAX_VALUE}, 100);
    private final ReservoirBucket graphSearchRowsReservoir = new ReservoirBucket(new int[] {10, 100, 250, 500, Integer.MAX_VALUE}, 100);
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;

    private final boolean useStatsForBruteForce = CassandraRelevantProperties.VECTOR_INDEX_SEARCHER_TRACK_STATS_FOR_BRUTE_FORCE.getBoolean();

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
                                    JVectorLuceneOnDiskGraph graph)
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);
        this.graph = graph;
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));

        globalBruteForceRows = Integer.MAX_VALUE;
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public RangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        return searchPosting(context, exp, keyRange, limit);
    }

    private RangeIterator searchPosting(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN && exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        if (exp.getEuclideanSearchThreshold() > 0)
            limit = 100000;
        int topK = topKFor(limit);
        float[] queryVector = exp.lower.value.vector;

        long startNanos = System.nanoTime();

        BitsOrPostingList bitsOrPostingList = bitsOrPostingListForKeyRange(context, keyRange, queryVector, topK);
        if (bitsOrPostingList.skipANN())
        {
            return toPrimaryKeyIterator(bitsOrPostingList.postingList(),
                                        context,
                                        createListener(System.nanoTime() - startNanos,
                                                       bitsOrPostingList.getNumScannedRows(),
                                                       bruteForceRowsReservoir.get(limit)));
        }

        var vectorPostings = graph.search(queryVector, topK, exp.getEuclideanSearchThreshold(), limit, bitsOrPostingList.getBits(), context);
        if (bitsOrPostingList.rawExpectedNodes >= 0)
            updateExpectedNodes(vectorPostings.getVisitedCount(), bitsOrPostingList.rawExpectedNodes);
        return toPrimaryKeyIterator(vectorPostings,
                                    context,
                                    createListener(System.nanoTime() - startNanos,
                                                   vectorPostings.getVisitedCount(),
                                                   graphSearchRowsReservoir.get(limit)));
    }

    /**
     * @return the topK >= `limit` results to ask the index to search for.  This allows
     * us to compensate for using lossily-compressed vectors during the search, by
     * searching deeper in the graph.
     */
    private int topKFor(int limit)
    {
        var cv = graph.getCompressedVectors();
        // uncompressed indexes don't need to over-search
        if (cv == null)
            return limit;

        // compute the factor `n` to multiply limit by to increase the number of results from the index.
        var n = 0.509 + 9.491 * pow(limit, -0.402); // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1
        // The function becomes less than 1 at limit ~= 1583.4
        n = max(1.0, n);

        // 2x results at limit=100 is enough for all our tested data sets to match uncompressed recall,
        // except for the ada002 vectors that compress at a 32x ratio.  For ada002, we need 3x results
        // with PQ, and 4x for BQ.
        if (cv instanceof BinaryQuantization)
            n *= 2;
        else if ((double) cv.getOriginalSize() / cv.getCompressedSize() > 16.0)
            n *= 1.5;

        return (int) (n * limit);
    }

    /**
     * Return bit set if needs to search HNSW; otherwise return posting list to bypass HNSW
     */
    private BitsOrPostingList bitsOrPostingListForKeyRange(QueryContext context,
                                                           AbstractBounds<PartitionPosition> keyRange,
                                                           float[] queryVector,
                                                           int topK) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.nextAfter(firstPrimaryKey);
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return new BitsOrPostingList(PostingList.EMPTY, 0);
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return new BitsOrPostingList(PostingList.EMPTY, 0);

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // If num of matches are not bigger than topK, skip ANN.
            // (nRows should not include shadowed rows, but context doesn't break those out by segment,
            // so we will live with the inaccuracy.)
            final int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            boolean useBruteForce = recommendBruteForce(topK, nRows);
            logAndTrace("Search range covers {} rows; will{}use brute force for sstable index with {} nodes, LIMIT {}",
                        nRows, useBruteForce ? " " : " not ", graph.size(), topK);

            var sstableRowIdStream = LongStream.range(minSSTableRowId, maxSSTableRowId + 1)
                                       .filter(sstableRowId -> context.shouldInclude(sstableRowId, primaryKeyMap));
            // if we have a small number of results then let TopK processor do exact NN computation
            if (useBruteForce)
            {
                var segmentRowIds = new IntArrayList(nRows, 0);
                sstableRowIdStream.forEach(sstableRowId -> segmentRowIds.add(metadata.toSegmentRowId(sstableRowId)));
                var postings = findTopApproximatePostings(queryVector, segmentRowIds, topK);
                return new BitsOrPostingList(new ArrayPostingList(postings), nRows);
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            AtomicBoolean hasMatches = new AtomicBoolean(false);
            try (var ordinalsView = graph.getOrdinalsView())
            {
                sstableRowIdStream.forEach(sstableRowId ->
                   {
                       int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                       int ordinal = getOrdinal(ordinalsView, segmentRowId);
                       if (ordinal >= 0)
                       {
                           bits.set(ordinal);
                           hasMatches.set(true);
                       }
                   });
            }
            catch (RuntimeException rte)
            {
                // getOrdinal rethrows IOException as RTE
                if (rte.getCause() instanceof IOException)
                    throw (IOException) rte.getCause();
                throw rte;
            }

            if (!hasMatches.get())
                return new BitsOrPostingList(PostingList.EMPTY, nRows);

            return new BitsOrPostingList(bits, getRawExpectedNodes(topK, nRows));
        }
    }

    private static int getOrdinal(OrdinalsView ordinalsView, int segmentRowId)
    {
        try
        {
            return ordinalsView.getOrdinalForRowId(segmentRowId);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private int[] findTopApproximatePostings(float[] queryVector, IntArrayList segmentRowIds, int topK) throws IOException
    {
        var cv = graph.getCompressedVectors();
        if (cv == null || segmentRowIds.size() <= topK)
            return segmentRowIds.toIntArray();

        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        var scoreFunction = cv.approximateScoreFunctionFor(queryVector, similarityFunction);

        ArrayList<SearchResult.NodeScore> pairs = new ArrayList<>(segmentRowIds.size());
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                var score = scoreFunction.similarityTo(ordinal);
                pairs.add(new SearchResult.NodeScore(segmentRowId, score));
            }
        }
        // sort descending
        pairs.sort((a, b) -> Float.compare(b.score, a.score));
        int end = Math.min(pairs.size(), topK) - 1;
        int[] postings = new int[end + 1];
        // top K ascending
        for (int i = end; i >= 0; i--)
            postings[end - i] = pairs.get(i).node;
        return postings;
    }

    private long getMaxSSTableRowId(PrimaryKeyMap primaryKeyMap, PartitionPosition right)
    {
        // if the right token is the minimum token, there is no upper bound on the keyRange and
        // we can save a lookup by using the maxSSTableRowId
        if (right.isMinimum())
            return metadata.maxSSTableRowId;

        PrimaryKey lastPrimaryKey = keyFactory.createTokenOnly(right.getToken());
        long max = primaryKeyMap.floor(lastPrimaryKey);
        if (max < 0)
            return metadata.maxSSTableRowId;
        return max;
    }

    private boolean recommendBruteForce(int limit, int nPermittedOrdinals)
    {
        if (nPermittedOrdinals > globalBruteForceRows)
            return false;

        int expectedNodes = expectedNodesVisited(limit, nPermittedOrdinals);

        if (useStatsForBruteForce)
        {
            var bfr = bruteForceRowsReservoir.get(limit);
            var gsr = graphSearchRowsReservoir.get(limit);
            // enough stats to guesstimate cost of brute force vs graph search
            if (bfr.size() > 10 && gsr.size() > 10)
            {
                var bruteForceCost = bfr.getMean() * nPermittedOrdinals;
                var graphSearchCost = gsr.getMean() * expectedNodes;

                if (logger.isDebugEnabled())
                    logger.debug("Estimated brute force cost: {} ns, graph search cost: {} ns, picking {}",
                                 bruteForceCost,
                                 graphSearchCost,
                                 bruteForceCost < graphSearchCost ? "brute force" : "graph search");

                // is brute force search cheaper?
                return bruteForceCost < graphSearchCost;
            }
        }
        // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
        // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
        // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
        // so I'm leaving it as a 1:1 ratio for now.
        return nPermittedOrdinals <= max(limit, expectedNodes);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals)
    {
        return (int) (getObservedNodesRatio() * getRawExpectedNodes(limit, nPermittedOrdinals));
    }

    /** the ratio of nodes visited by a graph search, to our estimate */
    private double getObservedNodesRatio()
    {
        return actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
    }

    private void updateExpectedNodes(int actualNodesVisited, int rawExpectedNodes)
    {
        assert rawExpectedNodes >= 0 : rawExpectedNodes;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        double ratio = getObservedNodesRatio();
        var expectedNodes = (int) (ratio * rawExpectedNodes);
        if (actualNodesVisited >= 1000 && (actualNodesVisited > 2 * expectedNodes || actualNodesVisited < 0.5 * expectedNodes))
            logger.warn("Predicted visiting {} nodes, but actually visited {} (observed:predicted ratio is {})",
                        expectedNodes, actualNodesVisited, ratio);
        actualExpectedRatio.updateAndGet(actualNodesVisited, rawExpectedNodes);
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        var bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    @Override
    public RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        // create a sublist of the keys within this segment's bounds
        int minIndex = Collections.binarySearch(keys, metadata.minKey);
        minIndex = minIndex < 0 ? -minIndex - 1 : minIndex;
        int maxIndex = Collections.binarySearch(keys, metadata.maxKey);
        maxIndex = maxIndex < 0 ? -maxIndex - 1 : maxIndex + 1;
        List<PrimaryKey> keysInRange = keys.subList(minIndex, maxIndex);
        if (keysInRange.isEmpty())
            return RangeIterator.empty();

        logAndTrace("SAI predicates produced {} rows out of limit {}", keysInRange.size(), limit);
        if (keysInRange.size() <= limit)
            return new ListRangeIterator(metadata.minKey, metadata.maxKey, keysInRange);

        int topK = topKFor(limit);
        // if we are brute forcing, we want to build a list of segment row ids, but if not,
        // we want to build a bitset of ordinals corresponding to the rows.  We won't know which
        // path to take until we have an accurate key count.
        SparseFixedBitSet bits = bitSetForSearch();
        IntArrayList rowIds = new IntArrayList();
        try (var primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
             var ordinalsView = graph.getOrdinalsView())
        {
            for (PrimaryKey primaryKey : keysInRange)
            {
                long sstableRowId = primaryKeyMap.exactRowIdForPrimaryKey(primaryKey);
                if (sstableRowId < 0)
                    continue;

                // these should still be true based on our computation of keysInRange
                assert sstableRowId >= metadata.minSSTableRowId : String.format("sstableRowId %d < minSSTableRowId %d", sstableRowId, metadata.minSSTableRowId);
                assert sstableRowId <= metadata.maxSSTableRowId : String.format("sstableRowId %d > maxSSTableRowId %d", sstableRowId, metadata.maxSSTableRowId);

                // add it to the list of rows to search if it has a vector associated with it
                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                {
                    rowIds.add(segmentRowId);
                    bits.set(ordinal);
                }
            }
        }

        var numRows = rowIds.size();
        if (numRows == 0) {
            logAndTrace("0 rows relevant to current sstable");
            return RangeIterator.empty();
        }

        boolean useBruteForce = recommendBruteForce(topK, numRows);
        logAndTrace("{} rows relevant to current sstable; will{}use brute force for index with {} nodes, LIMIT {}",
                    numRows, useBruteForce ? " " : " not ", graph.size(), limit);

        long startNanos = System.nanoTime();
        if (useBruteForce)
        {
            // brute force using the in-memory compressed vectors to cut down the number of results returned
            var queryVector = exp.lower.value.vector;
            var postings = findTopApproximatePostings(queryVector, rowIds, topK);
            return toPrimaryKeyIterator(new ArrayPostingList(postings), context, createListener(System.nanoTime() - startNanos, numRows, bruteForceRowsReservoir.get(limit)));
        }
        // else ask the index to perform a search limited to the bits we created
        float[] queryVector = exp.lower.value.vector;
        var results = graph.search(queryVector, topK, limit, bits, context);
        updateExpectedNodes(results.getVisitedCount(), getRawExpectedNodes(topK, numRows));
        return toPrimaryKeyIterator(results, context, createListener(System.nanoTime() - startNanos, results.getVisitedCount(), graphSearchRowsReservoir.get(limit)));
    }

    PostingListRangeIterator.StartStopListener createListener(long elapsedTime, int numVisisted, Reservoir reservoir)
    {
        return new PostingListRangeIterator.StartStopListener()
        {
            private long startedNanos = 0;
            @Override
            public void doOnStart() {
                startedNanos = System.nanoTime();
            }

            @Override
            public void doOnStop() {
                if (numVisisted == 0)
                    return;

                long elapsedNanos = elapsedTime + System.nanoTime() - startedNanos;
                reservoir.update(elapsedNanos / numVisisted);
            }
        };
    }
    {

    }

    private int getRawExpectedNodes(int topK, int nPermittedOrdinals)
    {
        return VectorMemtableIndex.expectedNodesVisited(topK, nPermittedOrdinals, graph.size());
    }

    private void logAndTrace(String message, Object... args)
    {
        logger.trace(message, args);
        Tracing.trace(message, args);
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
        private final int rawExpectedNodes;
        private final PostingList postingList;
        private final int rowsScanned;

        public BitsOrPostingList(Bits bits, int rawExpectedNodes)
        {
            this.bits = bits;
            this.rawExpectedNodes = rawExpectedNodes;
            this.postingList = null;
            this.rowsScanned = -1;
        }

        public BitsOrPostingList(Bits bits)
        {
            this.bits = bits;
            this.postingList = null;
            this.rawExpectedNodes = -1;
            this.rowsScanned = -1;
        }

        public BitsOrPostingList(PostingList postingList, int rowsScanned)
        {
            this.bits = null;
            this.postingList = Preconditions.checkNotNull(postingList);
            this.rawExpectedNodes = -1;
            this.rowsScanned = rowsScanned;
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

        public int getNumScannedRows()
        {
            Preconditions.checkState(skipANN());
            return rowsScanned;
        }

        public boolean skipANN()
        {
            return postingList != null;
        }
    }
}
