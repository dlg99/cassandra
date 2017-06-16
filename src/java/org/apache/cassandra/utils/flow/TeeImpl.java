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
package org.apache.cassandra.utils.flow;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implementation of {@link CsFlow.Tee} based on waiting for all clients to make requests before sending one upstream
 * and forwarding the onNext call to all clients.
 * Clients can individually close their stream, in which case a request from them is no longer necessary.
 *
 * This tee cannot be used to predicate the execution of one branch on the other completing, e.g.
 *     tee.child(0)
 *        .doSomething()
 *        .process()
 *        .flatMap(void -> tee.child(1))
 *        .doSomethingElse()
 * will not work because child(0) will not yield data until child(1) has requested, which in this case will never happen.
 *
 * However, something similar to the above can be achieved using
 *     tee.child(0)
 *        .doSomething()
 *        .reduceToFuture(...)
 *
 *     tee.child(1)
 *        .doSomethingElse()
 *
 * This version will execute requests and onNext on the first client together with (and in the same thread as) the
 * requests and onNexts of the second client. If the second client closes, this would trigger execution of the remainder
 * of the requests of the first client.
 */
public class TeeImpl<T> implements CsSubscriber<T>, CsFlow.Tee<T>
{
    private final CsFlow<T> sourceFlow;
    private CsSubscription source;
    private final TeeSubscription[] children;

    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();
    private final AtomicInteger subscribed = new AtomicInteger();

    TeeImpl(CsFlow<T> source, int count) throws Exception
    {
        this.sourceFlow = source;
        children = new TeeImpl.TeeSubscription[count];
        for (int i = 0; i < count; ++i)
            children[i] = new TeeSubscription(i);
    }

    public CsFlow<T> child(int i)
    {
        return children[i];
    }

    public void onNext(T item)
    {
        // None of these are allowed to throw, so we are okay not guarding with try/catch.
        for (TeeSubscription child : children)
            if (!child.closed)
                child.subscriber.onNext(item);
    }

    public void onComplete()
    {
        // None of these are allowed to throw, so we are okay not guarding with try/catch.
        for (TeeSubscription child : children)
            if (!child.closed)
                child.subscriber.onComplete();
    }

    public void onError(Throwable t)
    {
        // None of these are allowed to throw, so we are okay not guarding with try/catch.
        for (TeeSubscription child : children)
            if (!child.closed)
                child.subscriber.onError(t);
    }

    private void requestOne()
    {
        if (requests.incrementAndGet() < children.length)
            return;

        // We are currently serving requests from all non-closed children. They are not allowed to concurrently close,
        // so it's safe to copy without checking concurrent modification.
        requests.set(closed.get());

        source.request();
    }

    private void closeOne() throws Exception
    {
        if (closed.incrementAndGet() < children.length)
            requestOne();   // Reflect the closing in the number of requests we need.
        else
            source.close();
    }

    void subscribeOne() throws Exception
    {
        if (subscribed.incrementAndGet() < children.length)
            return;

        source = sourceFlow.subscribe(this);
    }

    public String toString()
    {
        return CsFlow.formatTrace("tee " + children.length + " ways") +
               Arrays.stream(children)
                     .map(child -> CsFlow.formatTrace("tee child", child.subscriber))
                     .collect(Collectors.joining("\n"));
    }

    class TeeSubscription extends CsFlow<T> implements CsSubscription
    {
        final int index;
        CsSubscriber<T> subscriber = null;
        volatile boolean closed = false;

        TeeSubscription(int index)
        {
            this.index = index;
        }

        public void request()
        {
            requestOne();
        }

        public void close() throws Exception
        {
            closed = true;
            closeOne();
        }

        public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
        {
            assert this.subscriber == null : "Tee is single-use.";
            subscribeOne();
            this.subscriber = subscriber;
            return this;
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }
    }
}
