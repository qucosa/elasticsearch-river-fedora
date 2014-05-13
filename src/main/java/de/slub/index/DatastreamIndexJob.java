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

import com.yourmediashelf.fedora.client.FedoraClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.concurrent.TimeUnit;

public class DatastreamIndexJob extends IndexJob {

    public DatastreamIndexJob(Type create, String pid, String dsid, int delay, TimeUnit unit) {
        super(create, pid, dsid, delay, unit);
    }

    public DatastreamIndexJob(Type type, String pid, String dsid) {
        super(type, pid, dsid);
    }

    @Override
    protected void executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) {
        log.warn("Datastream Delete not yet implemented.");
    }

    @Override
    protected void executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) {
        log.warn("Datastream Update not yet implemented.");
    }

    @Override
    protected void executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) {
        log.warn("Datastream Create not yet implemented.");
    }

}
