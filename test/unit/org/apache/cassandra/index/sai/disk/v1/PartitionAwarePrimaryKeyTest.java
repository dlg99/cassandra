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

package org.apache.cassandra.index.sai.disk.v1;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PrimaryKeyTester;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertTrue;

public class PartitionAwarePrimaryKeyTest extends PrimaryKeyTester
{
    public PartitionAwarePrimaryKeyTest()
    {
        super(Version.AA.onDiskFormat()::primaryKeyFactory);
    }

    @Test
    public void testAAFormatProducesPartitionAwarePrimaryKeyFactory()
    {
        assertTrue(super.factory instanceof PartitionAwarePrimaryKeyFactory);
    }
}
