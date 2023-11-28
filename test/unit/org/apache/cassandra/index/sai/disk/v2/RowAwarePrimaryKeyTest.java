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
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowAwarePrimaryKeyTest extends SAITester
{
    final PrimaryKey.Factory factory = Version.BA.onDiskFormat().primaryKeyFactory(EMPTY_COMPARATOR);

    @Test
    public void testBAFormatProducesRowAwarePrimaryKeyFactory()
    {
        // Tests in this class rely on this implementation detail
        assertTrue(factory instanceof RowAwarePrimaryKeyFactory);
    }

    @Test
    public void testHashCodeForDeffer()
    {
        // Set up the primary key
        Token token = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key = new BufferDecoratedKey(token, ByteBuffer.allocate(1));
        Supplier<PrimaryKey> supplier = () -> factory.create(key, Clustering.EMPTY);
        PrimaryKey primaryKey1 = factory.createDeferred(token, supplier);

        // Verify the results
        int hash1 = primaryKey1.hashCode();
        // Equals triggers loading the primary key
        assertEquals(primaryKey1, primaryKey1);
        assertEquals(hash1, primaryKey1.hashCode());

        // Do again with explicit loading
        PrimaryKey primaryKey2 = factory.createDeferred(token, supplier);
        int hash2 = primaryKey2.hashCode();
        primaryKey2.loadDeferred();
        assertEquals(hash2, primaryKey2.hashCode());
    }

    @Test
    public void testCompareForDifferentTokens()
    {
        Token token1 = new Murmur3Partitioner.LongToken(1);
        Token token2 = new Murmur3Partitioner.LongToken(2);

        // Create token-only primary key
        var pk1 = factory.createTokenOnly(token1);
        var pk2 = factory.createTokenOnly(token2);

        assertEquals(-1, pk1.compareTo(pk2));
        assertEquals(1, pk2.compareTo(pk1));

        assertEquals(-1, pk1.compareComparableBytes(pk2));
        assertEquals(1, pk2.compareComparableBytes(pk1));
    }

    @Test
    public void testCompareComparableBytesForSameTokenPrimaryKeys()
    {
        Token token = new Murmur3Partitioner.LongToken(1);

        // Create token-only primary key
        var tokenOnlyPrimaryKey = factory.createTokenOnly(token);

        // Create primary key with partition only of byte 0
        DecoratedKey key1 = new BufferDecoratedKey(token, ByteBuffer.allocate(1).put(0, (byte) 0));
        PrimaryKey primaryKey1 = factory.createPartitionKeyOnly(key1);

        // verify compareTo implementation
        assertEquals(0, tokenOnlyPrimaryKey.compareTo(primaryKey1));
        assertEquals(0, tokenOnlyPrimaryKey.compareTo(tokenOnlyPrimaryKey));
        assertEquals(0, primaryKey1.compareTo(primaryKey1));
        assertEquals(0, primaryKey1.compareTo(tokenOnlyPrimaryKey));

        // verify byte compareComparableBytes
        assertEquals(-1, tokenOnlyPrimaryKey.compareComparableBytes(primaryKey1));
        assertEquals(0, tokenOnlyPrimaryKey.compareComparableBytes(tokenOnlyPrimaryKey));
        assertEquals(0, primaryKey1.compareComparableBytes(primaryKey1));
        assertEquals(1, primaryKey1.compareComparableBytes(tokenOnlyPrimaryKey));

        // Verify that compareComparableBytes matches the ByteComparable.compare implementation
        assertEquals(compareBytes(tokenOnlyPrimaryKey, primaryKey1), tokenOnlyPrimaryKey.compareComparableBytes(primaryKey1));
        assertEquals(compareBytes(primaryKey1, primaryKey1), primaryKey1.compareComparableBytes(primaryKey1));
        assertEquals(compareBytes(tokenOnlyPrimaryKey, tokenOnlyPrimaryKey), tokenOnlyPrimaryKey.compareComparableBytes(tokenOnlyPrimaryKey));
        assertEquals(compareBytes(primaryKey1, tokenOnlyPrimaryKey), primaryKey1.compareComparableBytes(tokenOnlyPrimaryKey));

        // Verify results for a primary key with a different partition key
        DecoratedKey key2 = new BufferDecoratedKey(token, ByteBuffer.allocate(1).put(0, (byte) 1));
        PrimaryKey primaryKey2 = factory.createPartitionKeyOnly(key2);

        // verify byte compareComparableBytes for different partition keys
        assertEquals(-1, primaryKey1.compareComparableBytes(primaryKey2));
        assertEquals(1, primaryKey2.compareComparableBytes(primaryKey1));

        // Verify that compareComparableBytes matches the ByteComparable.compare implementation
        assertEquals(compareBytes(primaryKey1, primaryKey2), primaryKey1.compareComparableBytes(primaryKey2));
        assertEquals(compareBytes(primaryKey2, primaryKey1), primaryKey2.compareComparableBytes(primaryKey1));
    }

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

    private int compareBytes(PrimaryKey pk1, PrimaryKey pk2)
    {
        // We want to compare the bytes using all versions and then assert that we have just one result to ensure that
        // the byte comparable representations are internally consistent across versions.
        var results = Arrays.stream(ByteComparable.Version.values())
                            .map(version -> ByteComparable.compare(pk1::asComparableBytes, pk2::asComparableBytes, version))
                            .collect(Collectors.toSet());
        assertEquals(1, results.size());
        return results.iterator().next();
    }
}
