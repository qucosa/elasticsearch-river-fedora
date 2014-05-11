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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.concurrent.TimeUnit;

public class ObjectIndexJob extends IndexJob {

    public ObjectIndexJob(Type type, String pid, int delay, TimeUnit unit) {
        super(type, pid, delay, unit);
    }

    public ObjectIndexJob(Type type, String pid) {
        super(type, pid);
    }

    @Override
    protected void executeDelete(Client client, ESLogger log) {
        log.warn("Object Delete not yet implemented.");
    }

    @Override
    protected void executeUpdate(Client client, ESLogger log) {
        log.warn("Object Update not yet implemented.");
    }

    @Override
    protected void executeCreate(Client client, ESLogger log) {
        log.warn("Object Create not yet implemented.");
    }

}
