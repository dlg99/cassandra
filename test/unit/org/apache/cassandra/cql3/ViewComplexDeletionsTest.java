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

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.ViewComplexTest.createView;
import static org.apache.cassandra.cql3.ViewComplexTest.updateView;
import static org.apache.cassandra.cql3.ViewComplexTest.updateViewWithFlush;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 */
@RunWith(Parameterized.class)
public class ViewComplexDeletionsTest extends CQLTester
{
    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ViewComplexTest.versions();
    }

    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        ViewComplexTest.startup();
    }

    @Before
    public void begin()
    {
        ViewComplexTest.beginSetup(views);
    }

    @After
    public void end() throws Throwable
    {
        ViewComplexTest.endTearDown(views, version, this);
    }

    // for now, unselected column cannot be fully supported, SEE CASSANDRA-11500
    @Ignore
    @Test
    public void testPartialDeleteUnselectedColumn() throws Throwable
    {
        boolean flush = true;
        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY (k, c))");
        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT k,c FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)",
                   version,
                   this,
                   views);
        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 10 SET b=1 WHERE k=1 AND c=1", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertRows(execute("SELECT * from %s"), row(1, 1, null, 1));
        assertRows(execute("SELECT * from mv"), row(1, 1));
        updateView("DELETE b FROM %s USING TIMESTAMP 11 WHERE k=1 AND c=1", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));
        updateView("UPDATE %s USING TIMESTAMP 1 SET a=1 WHERE k=1 AND c=1", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertRows(execute("SELECT * from %s"), row(1, 1, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1));

        execute("truncate %s;");

        // removal generated by unselected column should not shadow PK update with smaller timestamp
        updateViewWithFlush("UPDATE %s USING TIMESTAMP 18 SET a=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1));

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 20 SET a=null WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"));
        assertRows(execute("SELECT * from mv"));

        updateViewWithFlush("INSERT INTO %s(k,c) VALUES(1,1) USING TIMESTAMP 15", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1));
    }

    @Test
    public void testPartialDeleteSelectedColumnWithFlush() throws Throwable
    {
        testPartialDeleteSelectedColumn(true);
    }

    @Test
    public void testPartialDeleteSelectedColumnWithoutFlush() throws Throwable
    {
        testPartialDeleteSelectedColumn(false);
    }

    private void testPartialDeleteSelectedColumn(boolean flush) throws Throwable
    {
        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        createTable("CREATE TABLE %s (k int, c int, a int, b int, e int, f int, PRIMARY KEY (k, c))");
        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, k FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)",
                   version,
                   this,
                   views);
        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 10 SET b=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, 1, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, 1));

        updateViewWithFlush("DELETE b FROM %s USING TIMESTAMP 11 WHERE k=1 AND c=1", flush, version, this);
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 1 SET a=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, 1, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, 1, null));

        updateViewWithFlush("DELETE a FROM %s USING TIMESTAMP 1 WHERE k=1 AND c=1", flush, version, this);
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        // view livenessInfo should not be affected by selected column ts or tb
        updateViewWithFlush("INSERT INTO %s(k,c) VALUES(1,1) USING TIMESTAMP 0", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 12 SET b=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, 1, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, 1));

        updateViewWithFlush("DELETE b FROM %s USING TIMESTAMP 13 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateViewWithFlush("DELETE FROM %s USING TIMESTAMP 14 WHERE k=1 AND c=1", flush, version, this);
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        updateViewWithFlush("INSERT INTO %s(k,c) VALUES(1,1) USING TIMESTAMP 15", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateViewWithFlush("UPDATE %s USING TTL 3 SET b=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, 1, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, 1));

        TimeUnit.SECONDS.sleep(4);

        assertRows(execute("SELECT * from %s"), row(1, 1, null, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateViewWithFlush("DELETE FROM %s USING TIMESTAMP 15 WHERE k=1 AND c=1", flush, version, this);
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        execute("truncate %s;");

        // removal generated by unselected column should not shadow selected column with smaller timestamp
        updateViewWithFlush("UPDATE %s USING TIMESTAMP 18 SET e=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, null, null, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 18 SET e=null WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"));
        assertRows(execute("SELECT * from mv"));

        updateViewWithFlush("UPDATE %s USING TIMESTAMP 16 SET a=1 WHERE k=1 AND c=1", flush, version, this);
        assertRows(execute("SELECT * from %s"), row(1, 1, 1, null, null, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, 1, null));
    }

    @Test
    public void testRangeDeletionWithFlush() throws Throwable
    {
        testRangeDeletion(true);
    }

    @Test
    public void testRangeDeletionWithoutFlush() throws Throwable
    {
        testRangeDeletion(false);
    }

    public void testRangeDeletion(boolean flush) throws Throwable
    {
        // for partition range deletion, need to know that existing row is shadowed instead of not existed.
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        createView("mv_test1",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)",
                   version,
                   this,
                   views);

        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv_test1").disableAutoCompaction();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 0", 1, 1, 1, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 1));

        // remove view row
        updateView("UPDATE %s using timestamp 1 set b = null WHERE a=1", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        // remove base row, no view updated generated.
        updateView("DELETE FROM %s using timestamp 2 where a=1", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));

        // restor view row with b,c column. d is still tombstone
        updateView("UPDATE %s using timestamp 3 set b = 1,c = 1 where a=1", version, this); // upsert
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, null));
    }

    @Test
    public void testCommutativeRowDeletionFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(true);
    }

    @Test
    public void testCommutativeRowDeletionWithoutFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(false);
    }

    private void testCommutativeRowDeletion(boolean flush) throws Throwable
    {
        // CASSANDRA-13409 new update should not resurrect previous deleted data in view
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable-1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable-2
        updateView("Delete from %s using timestamp 2 where p = 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"));
        // sstable-3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
        // sstable-4
        updateView("UPdate %s using timestamp 4 set v1 = 2 where p = 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, null, null));
        // sstable-5
        updateView("UPdate %s using timestamp 5 set v1 = 1 where p = 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 4, 5;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
            List<String> sstables = cfs.getLiveSSTables()
                                       .stream()
                                       .sorted(Comparator.comparing(s -> s.descriptor.generation))
                                       .map(s -> s.getFilename())
                                       .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(3), sstables.get(4)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
            assertEquals(3, cfs.getLiveSSTables().size());
        }
        // regular tombstone should be retained after compaction
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
    }

    @Test
    public void testComplexTimestampDeletionTestWithFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(true);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(true);
    }

    @Test
    public void testComplexTimestampDeletionTestWithoutFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(false);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(false);
    }

    private void complexTimestampWithbasePKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key(p1, p2))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv2",
                   "create materialized view %s as select * from %%s where p1 is not null and p2 is not null primary key (p2, p1);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv2").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 1;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p1 = 1 and p2 = 2;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv2"));
        // insert PK with TS=3
        updateView("Insert into %s (p1, p2) values (1, 2) using timestamp 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));

        ks.getColumnFamilyStore("mv2").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));

        // reset values
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 10;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 10L));

        updateView("UPDATE %s using timestamp 20 SET v2 = 5 WHERE p1 = 1 and p2 = 2", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 5, 20L));

        updateView("DELETE FROM %s using timestamp 10 WHERE p1 = 1 and p2 = 2", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(null, 5, 20L));
    }

    public void complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 1;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(5, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p = 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv"));
        // insert PK with TS=3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 2;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));
        assertRowsIgnoringOrder(execute("SELECT * from mv limit 1"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        executeNet(version, "UPDATE %s USING TIMESTAMP 3 SET v2 = ? WHERE p = ?", 4, 3);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));

        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));
        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv limit 1"), row(1, 3, 4, 3L));
    }
}
