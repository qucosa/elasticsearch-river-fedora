/*
 * Copyright 2014 SLUB Dresden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.slub.util.concurrent;

import de.slub.util.Predicate;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UniquePredicateDelayQueueTest {

    @Test
    public void acceptsAllElementsWhenNoPredicateGiven() {
        UniquePredicateDelayQueue<DelayedQueueElement> queue = new UniquePredicateDelayQueue<>();
        assertTrue(queue.add(new DelayedQueueElement()));
    }

    @Test
    public void acceptsElementsAccordingToPredicate() {
        UniquePredicateDelayQueue<DelayedQueueElement> queue = new UniquePredicateDelayQueue<>();
        queue.addPredicate(
                new Predicate<DelayedQueueElement>() {
                    @Override
                    public boolean evaluate(DelayedQueueElement o) {
                        return true;
                    }
                }
        );
        assertTrue(queue.add(new DelayedQueueElement()));
    }

    @Test
    public void rejectsElementsAccordingToPredictate() {
        UniquePredicateDelayQueue<DelayedQueueElement> queue = new UniquePredicateDelayQueue<>();
        queue.addPredicate(
                new Predicate<DelayedQueueElement>() {
                    @Override
                    public boolean evaluate(DelayedQueueElement o) {
                        return false;
                    }
                }
        );
        assertFalse(queue.add(new DelayedQueueElement()));
    }

}
