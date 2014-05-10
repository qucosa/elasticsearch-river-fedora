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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedQueueElement implements Delayed {

    private long delay;
    private long origin;

    public DelayedQueueElement() {
        this(0, TimeUnit.MILLISECONDS);
    }

    public DelayedQueueElement(long delay, TimeUnit unit) {
        this.origin = System.currentTimeMillis();
        this.delay = unit.toMillis(delay);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delay - (System.currentTimeMillis() - origin), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o == this) return 0;

        long d = delay - o.getDelay(TimeUnit.MILLISECONDS);
        return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
    }

}
