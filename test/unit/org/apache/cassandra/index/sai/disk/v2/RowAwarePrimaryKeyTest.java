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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowAwarePrimaryKeyTest extends PrimaryKeyTester
{
    public RowAwarePrimaryKeyTest()
    {
        super(Version.BA.onDiskFormat()::primaryKeyFactory);
    }

    @Test
    public void testBAFormatProducesRowAwarePrimaryKeyFactory()
    {
        // Tests in this class rely on this implementation detail
        assertTrue(super.factory instanceof RowAwarePrimaryKeyFactory);
    }

    // Clustering columns are only covered by the RowAwarePrimaryKeyFactory right now, so this cannot go into
    // super's implementation
    @Test
    public void testCompareClusteringColumns()
    {
        var clusteringComparator = new ClusteringComparator(Int32Type.instance);
        var factory = Version.BA.onDiskFormat().primaryKeyFactory(clusteringComparator);

        // Test uses the same token and partition key because we're covering clustering columns only
        Token token = new Murmur3Partitioner.LongToken(1);
        DecoratedKey partitionKey = new BufferDecoratedKey(token, ByteBuffer.allocate(1));

        PrimaryKey partitionKeyOnly = factory.createPartitionKeyOnly(partitionKey);

        // Create clusterings with different values
        var clustering1 = Clustering.make(ByteBuffer.allocate(4).putInt(0, 1));
        var clustering2 = Clustering.make(ByteBuffer.allocate(4).putInt(0,2));

        PrimaryKey pk1 = factory.create(partitionKey, clustering1);
        PrimaryKey pk2 = factory.create(partitionKey, clustering2);

        // Verify regular compareTo
        assertEquals(-1, pk1.compareTo(pk2));
        assertEquals(0, partitionKeyOnly.compareTo(pk1));
        assertEquals(0, pk1.compareTo(partitionKeyOnly));
        assertEquals(1, pk2.compareTo(pk1));

        // Verify byte compareComparableBytes
        assertEquals(-1, pk1.compareComparableBytes(pk2));
        assertEquals(-1, partitionKeyOnly.compareComparableBytes(pk1));
        assertEquals(1, pk1.compareComparableBytes(partitionKeyOnly));
        assertEquals(1, pk2.compareComparableBytes(pk1));

        // Verify that compareComparableBytes matches the ByteComparable.compare implementation
        assertEquals(compareBytes(pk1, pk2), pk1.compareComparableBytes(pk2));
        assertEquals(compareBytes(partitionKeyOnly, pk1), partitionKeyOnly.compareComparableBytes(pk1));
        assertEquals(compareBytes(pk1, partitionKeyOnly), pk1.compareComparableBytes(partitionKeyOnly));
        assertEquals(compareBytes(pk2, pk1), pk2.compareComparableBytes(pk1));
    }
}
