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

package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.CollectionUtil;

public class ConjunctionPostingList implements PostingList
{
    final PostingList lead1, lead2;
    final PostingList[] others;

    private int hits = 0;
    private int misses = 0;

    private final List<PostingList> toClose;

    private final PostingList nonUniqueKeyPostings;
    private boolean unique = false;

    public ConjunctionPostingList(List<PostingList> lists, PostingList nonUniqueKeyPostings)
    {
        assert lists.size() >= 2;

        toClose = new ArrayList<>(lists);

        CollectionUtil.timSort(lists, (o1, o2) -> Long.compare(o1.size(), o2.size()));
        lead1 = lists.get(0);
        lead2 = lists.get(1);
        others = lists.subList(2, lists.size()).toArray(new PostingList[0]);
        this.nonUniqueKeyPostings = nonUniqueKeyPostings;
    }

    public boolean isUnique()
    {
        return unique;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(toClose);
    }

    private long doNext(long targetRowID) throws IOException
    {
        this.unique = false;
        advanceHead:
        for (;;)
        {
            assert !this.unique;

            if (nonUniqueKeyPostings != null)
            {
                final long nonUniqueRowid1 = nonUniqueKeyPostings.advance(targetRowID);
                if (nonUniqueRowid1 == targetRowID)
                {
                    // TODO: return that the row id exists in other sstables
                    this.unique = true;
                    return nonUniqueRowid1;
                }
            }

            assert targetRowID == lead1.currentPosting() : "targetRowID="+targetRowID+" lead1.currentPosting="+lead1.currentPosting();

            final long next2 = lead2.advance(targetRowID);

            if (nonUniqueKeyPostings != null)
            {
                final long nonUniqueRowID2 = nonUniqueKeyPostings.advance(next2);
                if (nonUniqueRowID2 == next2)
                {
                    // TODO: return that the row id exists in other sstables
                    this.unique = true;
                    return nonUniqueRowID2;
                }
            }

            // System.out.println("lead2.advance(targetRowID)="+targetRowID+" next2="+next2);

            if (next2 != targetRowID)
            {
                targetRowID = lead1.advance(next2);

                if (nonUniqueKeyPostings != null)
                {
                    final long nonUniqueRowID3 = nonUniqueKeyPostings.advance(targetRowID);
                    if (nonUniqueRowID3 == targetRowID)
                    {
                        // TODO: return that the row id exists in other sstables
                        this.unique = true;
                        return nonUniqueRowID3;
                    }
                }

                if (next2 != targetRowID)
                {
                    misses++;
                    continue;
                }
            }

            // then find agreement with other iterators
            for (PostingList other : others)
            {
                // other.targetRowID may already be equal to targetRowID if we "continued advanceHead"
                // on the previous iteration and the advance on the lead scorer exactly matched.
                if (other.currentPosting() < targetRowID)
                {
                    final long next = other.advance(targetRowID);

                    if (nonUniqueKeyPostings != null)
                    {
                        final long nonUniqueRowIDOther = nonUniqueKeyPostings.advance(targetRowID);
                        if (nonUniqueRowIDOther == next)
                        {
                            // TODO: return that the row id exists in other sstables
                            this.unique = true;
                            return nonUniqueRowIDOther;
                        }
                    }

                    if (next > targetRowID)
                    {
                        misses++;
                        // iterator beyond the current targetRowID - advance lead and continue to the new highest targetRowID.
                        targetRowID = lead1.advance(next);
                        continue advanceHead;
                    }
                }
            }

            // success - all iterators are on the same targetRowID
            // System.out.println("success - all iterators are on the same targetRowID="+targetRowID);
            hits++;
            return targetRowID;
        }
    }

    @Override
    public long currentPosting() {
        return lead1.currentPosting();
    }

    @Override
    public long size() {
        return lead1.size(); // overestimate
    }

    @Override
    public long advance(long target) throws IOException {
        return doNext(lead1.advance(target));
    }

    public long docID() {
        return lead1.currentPosting();
    }

    @Override
    public long nextPosting() throws IOException {
        return doNext(lead1.nextPosting());
    }
}
