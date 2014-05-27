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

import java.util.LinkedList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;

public class UniquePredicateDelayQueue<T extends Delayed> extends DelayQueue<T> {

    private LinkedList<Predicate<T>> predicates = new LinkedList<>();

    public UniquePredicateDelayQueue addPredicate(Predicate<T> predicate) {
        predicates.add(predicate);
        return this;
    }

    @Override
    public boolean offer(T e) {
        if (predicates.isEmpty() || evaluatePredicates(e)) {
            if (contains(e)) {
                remove(e);
            }
            return super.offer(e);
        } else {
            return false;
        }
    }

    private boolean evaluatePredicates(T e) {
        for (Predicate<T> p : predicates) {
            if (!p.evaluate(e)) return false;
        }
        return true;
    }

}
