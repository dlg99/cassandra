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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.jbellis.jvector.disk.CachingGraphIndex;
import com.github.jbellis.jvector.disk.CompressedVectors;
import com.github.jbellis.jvector.disk.OnDiskGraphIndex;
import com.github.jbellis.jvector.graph.GraphIndex;
import com.github.jbellis.jvector.graph.GraphSearcher;
import com.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import com.github.jbellis.jvector.graph.NeighborSimilarity;
import com.github.jbellis.jvector.graph.SearchResult.NodeScore;
import com.github.jbellis.jvector.util.Bits;
import com.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;

public class CassandraDiskAnn implements JVectorLuceneOnDiskGraph, AutoCloseable
{
    private static final Logger logger = Logger.getLogger(CassandraDiskAnn.class.getName());

    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;

    // only one of these will be not null
    private final CompressedVectors compressedVectors;
    private final ListRandomAccessVectorValues uncompressedVectors;

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
            if (containsCompressedVectors) {
                compressedVectors = CompressedVectors.load(reader, reader.getFilePointer());
                uncompressedVectors = null;
            }
            else
            {
                compressedVectors = null;
                var vectors = cacheOriginalVectors(graph);
                uncompressedVectors = new ListRandomAccessVectorValues(vectors, vectors.get(0).length);
            }
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    private static List<float[]> cacheOriginalVectors(GraphIndex<float[]> graph)
    {
        var view = graph.getView();
        return IntStream.range(0, graph.size()).mapToObj(view::getVector).collect(Collectors.toList());
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

        var searcher = new GraphSearcher.Builder<>(graph.getView()).build();
        NeighborSimilarity.ScoreFunction scoreFunction;
        NeighborSimilarity.ReRanker<float[]> reRanker;
        if (compressedVectors == null)
        {
            assert uncompressedVectors != null;
            scoreFunction = (NeighborSimilarity.ExactScoreFunction)
                            i -> similarityFunction.compare(queryVector, uncompressedVectors.vectorValue(i));
            reRanker = null;
        }
        else
        {
            scoreFunction = (NeighborSimilarity.ApproximateScoreFunction)
                            i -> compressedVectors.decodedSimilarity(i, queryVector, similarityFunction);
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
        private final RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

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
    public void close()
    {
        ordinalsMap.close();
        graph.close();
    }

    @Override
    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }
}
