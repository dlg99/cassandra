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

import java.util.List;
import java.util.function.ToIntFunction;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.plan.Expression;

/***
 * Analogue of SegmentOrdering, but for memtables.
 */
public interface MemtableOrdering
{
    /**
     * Filter the given list of {@link PrimaryKey} results to the top `limit` results corresponding to the given expression,
     * Returns an iterator over the results that is put back in token order.
     *
     * Assumes that the given  spans the same rows as the implementing index's segment.
     */
    default RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, ToIntFunction<Boolean> limit)
    {
        throw new UnsupportedOperationException();
    }
}
