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

package de.slub.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexJobTest {

    @Test
    public void testEquals() {
        IndexJob job1 = new IndexJob(IndexJob.Type.CREATE, "pid:1234", "dsid:1234");
        IndexJob job2 = new IndexJob(IndexJob.Type.CREATE, "pid:1234", "dsid:1234");

        assertTrue(job1.equals(job2));
        assertTrue(job2.equals(job1));
    }

    @Test
    public void testHascode() {
        IndexJob job1 = new IndexJob(IndexJob.Type.CREATE, "pid:1234", "dsid:1234");
        IndexJob job2 = new IndexJob(IndexJob.Type.CREATE, "pid:1234", "dsid:1234");

        assertEquals(job1.hashCode(), job2.hashCode());
    }

}