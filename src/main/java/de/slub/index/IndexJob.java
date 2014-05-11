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

import de.slub.util.concurrent.DelayedQueueElement;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.concurrent.TimeUnit;

public abstract class IndexJob extends DelayedQueueElement {

    private final Type type;
    private final String pid;
    private final String dsid;
    private final int hashCode;

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
        this.hashCode = new String(type + pid + dsid).hashCode();
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

    public void execute(Client client, ESLogger log) {
        switch (type) {
            case CREATE:
                executeCreate(client, log);
                break;
            case UPDATE:
                executeUpdate(client, log);
                break;
            case DELETE:
                executeDelete(client, log);
        }
    }

    protected abstract void executeDelete(Client client, ESLogger log);

    protected abstract void executeUpdate(Client client, ESLogger log);

    protected abstract void executeCreate(Client client, ESLogger log);

    public enum Type {
        CREATE,
        UPDATE,
        DELETE
    }

}
