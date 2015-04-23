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

package de.slub.index;

import de.slub.util.Predicate;

import java.util.List;

public class ExcludeDatastreamPredicate implements Predicate<IndexJob> {

    private final List<String> excludeDatastreams;

    public ExcludeDatastreamPredicate(List<String> excludeDatastreams) {
        this.excludeDatastreams = excludeDatastreams;
    }

    @Override
    public boolean evaluate(IndexJob job) {
        if (job instanceof DatastreamIndexJob) {
            if (!job.dsid().isEmpty()) {
                for (String excluded : excludeDatastreams) {
                    if (job.dsid().equals(excluded)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
