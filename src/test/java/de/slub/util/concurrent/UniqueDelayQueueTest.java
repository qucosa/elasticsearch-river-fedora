/*
 * Copyright 2014 SLUB Dresden
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package de.slub.util.concurrent;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UniqueDelayQueueTest {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);
    private UniqueDelayQueue<Delayed> queue;

    @Before
    public void setUp() {
        queue = new UniqueDelayQueue<>();
    }

    @After
    public void tearDown() {
        queue.clear();
    }

    @Test
    public void enqueueingSameElementOnlyOnce() {
        Delayed element = new DelayedQueueElement();
        queue.add(element);
        queue.add(element);

        assertEquals(1, queue.size());
    }

    @Test
    public void elementNotAvailableBeforeDelayTimeElapsed() {
        Delayed element = new DelayedQueueElement(5, TimeUnit.MILLISECONDS);
        queue.add(element);

        assertNull(queue.poll());
    }

    @Test
    public void elementIsAvailableAfterDelayTime() throws InterruptedException {
        Delayed element = new DelayedQueueElement(10, TimeUnit.MILLISECONDS);
        queue.add(element);

        assertEquals(element, queue.take());
    }

    @Test
    public void elementIsBeReadyPriorToOtherElement() {
        Delayed first = new DelayedQueueElement(3, TimeUnit.MILLISECONDS);
        Delayed second = new DelayedQueueElement(1, TimeUnit.MILLISECONDS);
        queue.add(first);
        queue.add(second);

        assertEquals(second, queue.peek());
    }

}
