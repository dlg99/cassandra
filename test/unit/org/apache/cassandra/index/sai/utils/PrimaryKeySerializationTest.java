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

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertTrue;

public class PrimaryKeySerializationTest
{
    @BeforeClass
    public static void initialise() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void skinnyRowTest() throws Exception
    {
        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance,
                                                                     new ClusteringComparator(),
                                                                     Version.LATEST.onDiskFormat().indexFeatureSet());
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));
        PrimaryKey expected = keyFactory.createKey(decoratedKey);

        DataOutputBuffer output = new DataOutputBuffer();

        PrimaryKey.serializer.serialize(output, 0, expected);

        DataInputBuffer input = new DataInputBuffer(output.toByteArray());

        PrimaryKey result = keyFactory.createKey(input, 1);

        assertTrue(expected.compareTo(result) == 0);
    }

    @Test
    public void test() throws Exception
    {
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));
        ByteBuffer[] values = new ByteBuffer[1];
        values[0] = UTF8Type.instance.decompose("B");
        Clustering clustering = Clustering.make(values);

        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance,
                                                                     new ClusteringComparator(UTF8Type.instance),
                                                                     Version.LATEST.onDiskFormat().indexFeatureSet());

        PrimaryKey expected = keyFactory.createKey(decoratedKey, clustering);

        DataOutputBuffer output = new DataOutputBuffer();

        PrimaryKey.serializer.serialize(output, 0, expected);

        DataInputBuffer input = new DataInputBuffer(output.toByteArray());

        PrimaryKey result = keyFactory.createKey(input, 1);

        assertTrue(expected.compareTo(result) == 0);
    }
}