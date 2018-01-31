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

package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.concurrent.TPCTaskType;

public class TpStatsPrinter
{
    public static StatsPrinter from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                return new DefaultPrinter();
        }

    }

    public static class DefaultPrinter implements StatsPrinter<TpStatsHolder>
    {
        @Override
        public void print(TpStatsHolder data, PrintStream out)
        {
            Map<String, Object> convertData = data.convert2Map();

            String headerFormat = "%-" + longestTPCStatNameLength() + "s%12s%30s%10s%15s%10s%18s%n";
            out.printf(headerFormat, "Pool Name", "Active", "Pending (w/Backpressure)", "Delayed", "Completed", "Blocked", "All time blocked");

            Map<Object, Object> threadPools = convertData.get("ThreadPools") instanceof Map<?, ?> ? (Map)convertData.get("ThreadPools") : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : threadPools.entrySet())
            {
                Map values = entry.getValue() instanceof Map<?, ?> ? (Map)entry.getValue() : Collections.emptyMap();
                out.printf(headerFormat,
                           entry.getKey(),
                           values.get("ActiveTasks"),
                           values.get("PendingTasks"),
                           values.get("DelayedTasks"),
                           values.get("CompletedTasks"),
                           values.get("CurrentlyBlockedTasks"),
                           values.get("TotalBlockedTasks"));
            }

            Map<Object, Object> droppedMessages = convertData.get("DroppedMessage") instanceof Map<?, ?> ? (Map)convertData.get("DroppedMessage") : Collections.emptyMap();
            int messageTypeIndent = longestStrLength(droppedMessages.keySet()) + 5;

            out.printf("%n%-" + messageTypeIndent + "s%10s%18s%18s%18s%18s%n", "Message type", "Dropped", "", "Latency waiting in queue (micros)", "", "");
            out.printf("%-" + messageTypeIndent + "s%10s%18s%18s%18s%18s%n", "", "", "50%", "95%", "99%", "Max");
            Map<Object, double[]> waitLatencies = convertData.get("WaitLatencies") instanceof Map<?, ?> ? (Map)convertData.get("WaitLatencies") : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : droppedMessages.entrySet())
            {
                out.printf("%-" + messageTypeIndent + "s%10s", entry.getKey(), entry.getValue());
                if (waitLatencies.containsKey(entry.getKey()))
                {
                    double[] latencies = waitLatencies.get(entry.getKey());
                    out.printf("%18.2f%18.2f%18.2f%18.2f", latencies[0], latencies[2], latencies[4], latencies[6]);
                }
                else
                {
                    out.printf("%18s%18s%18s%18s", "N/A", "N/A", "N/A", "N/A");
                }

                out.printf("%n");
            }
        }
    }

    public static int longestTPCStatNameLength()
    {
        return longestStrLength(Arrays.stream(TPCTaskType.values())
                                      .map(t -> t.loggedEventName)
                                      .collect(Collectors.toList()))
                + 10; // or "TPC/other/".length
    }

    public static int longestStrLength(Collection<Object> coll)
    {
        int maxLength = 0;
        for (Object o : coll)
            maxLength = Integer.max(maxLength, o.toString().length());
        return maxLength;
    }
}
