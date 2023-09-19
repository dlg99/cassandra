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
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.CompressedVectors;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.SearchResult.NodeScore;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;

public class CassandraDiskAnn implements JVectorLuceneOnDiskGraph, AutoCloseable
{
    private static final Logger logger = Logger.getLogger(CassandraDiskAnn.class.getName());

    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;
    @Nullable
    private final CompressedVectors compressedVectors;

    public CassandraDiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        graph = new CachingGraphIndex(new OnDiskGraphIndex<>(() -> indexFiles.termsData().createReader(), termsMetadata.offset));

        long pqSegmentOffset = componentMetadatas.get(IndexComponent.PQ).offset;
        try (var reader = indexFiles.pq().createReader())
        {
            reader.seek(pqSegmentOffset);
            var containsCompressedVectors = reader.readBoolean();
            if (containsCompressedVectors)
                compressedVectors = CompressedVectors.load(reader, reader.getFilePointer());
            else
                compressedVectors = null;
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    @Override
    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public int size()
    {
        return graph.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    // VSTODO make this return something with a size
    @Override
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits, QueryContext context)
    {
        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        var view = graph.getView();
        var searcher = new GraphSearcher.Builder<>(view).build();
        NeighborSimilarity.ScoreFunction scoreFunction;
        NeighborSimilarity.ReRanker<float[]> reRanker;
        if (compressedVectors == null)
        {
            scoreFunction = (NeighborSimilarity.ExactScoreFunction)
                            i -> similarityFunction.compare(queryVector, view.getVector(i));
            reRanker = null;
        }
        else
        {
            scoreFunction = compressedVectors.approximateScoreFunctionFor(queryVector, similarityFunction);
            reRanker = (i, map) -> similarityFunction.compare(queryVector, map.get(i));
        }
        var result = searcher.search(scoreFunction,
                                     reRanker,
                                     topK,
                                     ordinalsMap.ignoringDeleted(acceptBits));
        return annRowIdsToPostings(result.getNodes());
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final Iterator<NodeScore> it;
        private final OnDiskOrdinalsMap.RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NodeScore[] results)
        {
            this.it = Arrays.stream(results).iterator();
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && it.hasNext()) {
                try
                {
                    var ordinal = it.next().node;
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

    private ReorderingPostingList annRowIdsToPostings(NodeScore[] results)
    {
        try (var iterator = new RowIdIterator(results))
        {
            return new ReorderingPostingList(iterator, results.length);
        }
    }

    @Override
    public void close() throws IOException
    {
        ordinalsMap.close();
        graph.close();
    }

    @Override
    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }
}