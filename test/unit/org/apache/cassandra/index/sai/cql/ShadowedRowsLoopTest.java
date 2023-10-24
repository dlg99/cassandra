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

package org.apache.cassandra.index.sai.cql;


import java.util.Arrays;
import javax.management.JMX;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.assertj.core.data.Percentage;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(Parameterized.class)
public class ShadowedRowsLoopTest extends VectorTester
{
    private static final String PER_QUERY_METRIC_TYPE = "PerQuery";

    final int vectorCount = 100;
    final int dimension = 11;
    final int limit = 5;
    final int N;
    private Vector<Float> queryVector;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        startJMXServer();
        createMBeanServerConnection();
    }

    @Parameterized.Parameters
    public static Object[] data()
    {
        return new Object[] { 1, 3, 5, 10, 11, 13, 20, 50 };
        //return new Object[] { 5 };
    }

    public ShadowedRowsLoopTest(int N)
    {
        this.N = N;
    }

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();

        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // insert records with pk starting at vectorCount, flush
        // these will be returned by search
        for (int i = 0; i < vectorCount; i++)
        {
            this.queryVector = randomVector(vectorCount + i + 1, dimension);
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                    vectorCount + i, Integer.toString(i), queryVector);
        }
        flush();

        //insert/delete records to force shadowed rows loop
        for (int loop = 0; loop < N; loop++)
        {
            // insert records with pk starting at 0 and < vectorCount, flush
            for (int i = 0; i < vectorCount; i++)
            {
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                        i, Integer.toString(i), randomVector(i + 1, dimension));
            }
            flush();

            // delete records with pk starting at 0 and < vectorCount, flush
            // now the records are shadowed
            for (int i = 0; i < vectorCount; i++)
            {
                execute("DELETE FROM %s WHERE pk = ?", i);
            }
            flush();
        }
    }

    @Test
    public void shadowedLoopTest() throws Throwable
    {

        QueryController.allowSpeculativeLimits.set(false);
        search(queryVector, limit);
        Metrics resultNoSp = getMetrics();
        assertThat(resultNoSp.loops).isGreaterThan(0);

        resetMetrics();

        QueryController.allowSpeculativeLimits.set(true);
        search(queryVector, limit);
        Metrics result = getMetrics();
        assertThat(result.loops).isGreaterThan(0);

        logger.info("N: {}; loops {} -> {}, keys {} -> {}",
                    N, resultNoSp.loops, result.loops, resultNoSp.keys, result.keys);

        assertThat(result.loops).isLessThan(resultNoSp.loops);
    }

    private Metrics getMetrics() throws InterruptedException
    {
        long prev = -1;
        long loops = -1;
        long keys = -1;

        // poll for metric to be updated
        for (int i = 0; i < 100; i++)
        {
            var loopsMetric = getQueryHistogram("ShadowedKeysLoopsHistogram");
            var keysMetric = getQueryHistogram("ShadowedKeysScannedHistogram");
            loops = loopsMetric.getMax();
            keys = keysMetric.getMax();

            if (loops > 0 && loops == prev)
                break;
            prev = loops;
            loops = -1;
            keys = -1;
            Thread.sleep(300);
        }
        return new Metrics(loops, keys);
    }

    private void resetMetrics() throws InterruptedException
    {
        long loops;
        getQueryHistogram("ShadowedKeysLoopsHistogram").clear();
        getQueryHistogram("ShadowedKeysScannedHistogram").clear();

        // poll for metric to be reset
        for (int i = 0; i < 50; i++)
        {
            var loopsNoSpMetric = getQueryHistogram("ShadowedKeysLoopsHistogram");
            loops = loopsNoSpMetric.getMax();

            if (loops == 0)
                break;
            Thread.sleep(300);
        }
        assertThat(getQueryHistogram("ShadowedKeysLoopsHistogram").getMax()).isEqualTo(0);
    }

    private static class Metrics
    {
        public final long loops;
        public final long keys;

        public Metrics(long loops, long keys)
        {
            this.loops = loops;
            this.keys = keys;
        }
    }

    private UntypedResultSet search(Vector<Float> queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT " + limit, queryVector);
        assertThat(result.size()).isCloseTo(limit, Percentage.withPercentage(5));
        return result;
    }

    private long getQueryMetrics(String metricsName) throws Exception
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace(), currentTable(), PER_QUERY_METRIC_TYPE));
    }

    private CassandraMetricsRegistry.JmxHistogramMBean getQueryHistogram(String metricName)
    {
        ObjectName oName = objectNameNoIndex(metricName, keyspace(), currentTable(), PER_QUERY_METRIC_TYPE);
        return JMX.newMBeanProxy(jmxConnection, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);
    }

    private static Vector<Float> randomVector(int x, int dimension)
    {
        Float[] rawVector = new Float[dimension];
        Arrays.fill(rawVector, (float) x);
        return new Vector<>(rawVector);
    }

    private Vector<Float> randomVector()
    {
        Float[] rawVector = new Float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return new Vector<>(rawVector);
    }
}
