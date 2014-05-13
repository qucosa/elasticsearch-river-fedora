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
import com.yourmediashelf.fedora.client.request.GetObjectProfile;
import com.yourmediashelf.fedora.client.response.GetObjectProfileResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ObjectIndexJob extends IndexJob {

    public ObjectIndexJob(Type type, String pid, int delay, TimeUnit unit) {
        super(type, pid, delay, unit);
    }

    public ObjectIndexJob(Type type, String pid) {
        super(type, pid);
    }

    @Override
    protected void executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) {
        log.warn("Object Delete not yet implemented.");
    }

    @Override
    protected void executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) {
        log.warn("Object Update not yet implemented.");
    }

    @Override
    protected void executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) {
        try {
            GetObjectProfileResponse profileResponse = (GetObjectProfileResponse)
                    fedoraClient.execute(new GetObjectProfile(pid()));
            XContentBuilder builder = jsonBuilder().startObject()
                    .field("PID", profileResponse.getPid())
                    .field("OBJ_STATE", profileResponse.getState())
                    .field("OBJ_DATE_CREATED", profileResponse.getCreateDate())
                    .field("OBJ_DATE_LAST_MODIFIED", profileResponse.getLastModifiedDate())
                    .field("OBJ_OWNER_ID", profileResponse.getOwnerId())
                    .field("OBJ_LABEL", profileResponse.getLabel());
            builder.endObject();
            client.prepareIndex(index(), indexType(), pid())
                    .setSource(builder)
                    .execute().actionGet();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
