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
import de.slub.util.concurrent.DelayedQueueElement;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.concurrent.TimeUnit;

public abstract class IndexJob extends DelayedQueueElement {

    private final Type type;
    private final String pid;
    private final String dsid;
    private final int hashCode;
    private String index;
    private String indexType;

    public IndexJob(Type type, String pid) {
        this(type, pid, "");
    }

    public IndexJob(Type type, String pid, long delay, TimeUnit unit) {
        this(type, pid, "", delay, unit);
    }

    public IndexJob(Type type, String pid, String dsid) {
        this(type, pid, dsid, 0, TimeUnit.MILLISECONDS);
    }

    public IndexJob(Type type, String pid, String dsid, long delay, TimeUnit unit) {
        super(delay, unit);
        this.type = type;
        this.pid = pid;
        this.dsid = dsid;
        this.hashCode = (type + pid + dsid).hashCode();
    }

    public String index() {
        return this.index;
    }

    public IndexJob index(String index) {
        this.index = index;
        return this;
    }

    public String indexType() {
        return this.indexType;
    }

    public IndexJob indexType(String indexType) {
        this.indexType = indexType;
        return this;
    }

    public String pid() {
        return pid;
    }

    public String dsid() {
        return dsid;
    }


    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        IndexJob other = (IndexJob) obj;
        return (other.pid.equals(this.pid)) &&
                (other.dsid.equals(this.dsid)) &&
                (other.type.equals(this.type));
    }

    @Override
    public String toString() {
        return String.format("[%s] [%s:%s]",
                type.toString(),
                pid, dsid);
    }

    public void execute(FedoraClient fedoraClient, Client client, ESLogger log)
            throws Exception {
        switch (type) {
            case CREATE:
                executeCreate(fedoraClient, client, log);
                break;
            case UPDATE:
                executeUpdate(fedoraClient, client, log);
                break;
            case DELETE:
                executeDelete(fedoraClient, client, log);
        }
    }

    protected abstract void executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception;

    protected abstract void executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception;

    protected abstract void executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception;

    public enum Type {
        CREATE,
        UPDATE,
        DELETE
    }

}
