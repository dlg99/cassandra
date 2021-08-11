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
package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Throwables;

/**
 * Interface for advancing on and consuming a posting list.
 */
@NotThreadSafe
public interface PostingList extends Closeable
{
    long OFFSET_NOT_FOUND = -1;
    long END_OF_STREAM = Long.MAX_VALUE;

    @Override
    default void close() throws IOException {}

    /**
     * Retrieves the next segment row ID, not including row IDs that have been returned by {@link #advance(PrimaryKey)}.
     *
     * @return next segment row ID
     */
    long nextPosting() throws IOException;

    default long currentPosting()
    {
        throw new UnsupportedOperationException("class="+getClass());
    }

    long size();

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     *
     * Note: Callers must use the return value of this method before calling {@link #nextPosting()}, as calling
     * that method will return the next posting, not the one to which we have just advanced.
     *
     * @param nextPrimaryKey target primary key to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */



    long advance(PrimaryKey nextPrimaryKey) throws IOException;

    long advance(long targetRowID) throws IOException;

//    default long advance(long targetRowID) throws IOException
//    {
//        return -1;
//    }

    //long advance(long target) throws IOException;

    PrimaryKey mapRowId(long rowId);

    /**
     * @return peekable wrapper of current posting list
     */
    default PeekablePostingList peekable()
    {
        return new PeekablePostingList(this);
    }

    public static class PeekablePostingList implements PostingList
    {
        private final PostingList wrapped;

        private boolean peeked = false;
        private long next;

        public PeekablePostingList(PostingList wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public long currentPosting()
        {
            return wrapped.currentPosting();
        }

        public long peek()
        {
            if (peeked)
                return next;

            try
            {
                peeked = true;
                return next = wrapped.nextPosting();
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        public long advanceWithoutConsuming(long targetRowID) throws IOException
        {
            if (peek() == END_OF_STREAM)
                return END_OF_STREAM;

            if (peek() >= targetRowID)
                return peek();

            peeked = true;
            next = wrapped.advance(targetRowID);
            return next;
        }

        public long advanceWithoutConsuming(PrimaryKey primaryKey) throws IOException
        {
            if (peek() == END_OF_STREAM)
                return END_OF_STREAM;

            if (wrapped.mapRowId(peek()).compareTo(primaryKey) >= 0)
                return peek();

            peeked = true;
            next = wrapped.advance(primaryKey);
            return next;
        }

        @Override
        public long nextPosting() throws IOException
        {
            if (peeked)
            {
                peeked = false;
                return next;
            }
            return wrapped.nextPosting();
        }

        @Override
        public long size()
        {
            return wrapped.size();
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            if (peeked && next >= targetRowID)
            {
                peeked = false;
                return next;
            }

            peeked = false;
            return wrapped.advance(targetRowID);
        }

        @Override
        public long advance(PrimaryKey primaryKey) throws IOException
        {
            if (peeked && wrapped.mapRowId(next).compareTo(primaryKey) >= 0)
            {
                peeked = false;
                return next;
            }

            peeked = false;

            return wrapped.advance(primaryKey);
        }

        @Override
        public PrimaryKey mapRowId(long rowId)
        {
            return wrapped.mapRowId(rowId);
        }

        @Override
        public void close() throws IOException
        {
            wrapped.close();
        }
    }
}
