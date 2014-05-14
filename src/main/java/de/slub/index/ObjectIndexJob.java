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
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.request.GetObjectProfile;
import com.yourmediashelf.fedora.client.response.GetObjectProfileResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ObjectIndexJob extends IndexJob {

    public static final String ES_TYPE_NAME = "object";

    public ObjectIndexJob(Type type, String pid, int delay, TimeUnit unit) {
        super(type, pid, delay, unit);
    }

    public ObjectIndexJob(Type type, String pid) {
        super(type, pid);
    }

    @Override
    public String indexType() {
        return ES_TYPE_NAME;
    }

    @Override
    protected void executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) {
        try {
            client.prepareDelete(index(), indexType(), pid()).execute().actionGet();
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Override
    protected void executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) {
        executeCreate(fedoraClient, client, log);
    }

    @Override
    protected void executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) {
        try {
            client.prepareIndex(index(), indexType(), pid())
                    .setSource(buildIndexObject(fedoraClient))
                    .execute().actionGet();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private XContentBuilder buildIndexObject(FedoraClient fedoraClient) throws IOException, FedoraClientException {
        GetObjectProfileResponse profileResponse = (GetObjectProfileResponse)
                fedoraClient.execute(new GetObjectProfile(pid()));
        XContentBuilder builder = jsonBuilder().startObject()
                .field("PID", profileResponse.getPid())
                .field("STATE", profileResponse.getState())
                .field("LABEL", profileResponse.getLabel())
                .field("OWNER_ID", profileResponse.getOwnerId())
                .field("CREATED_DATE", profileResponse.getCreateDate())
                .field("LAST_MODIFIED_DATE", profileResponse.getLastModifiedDate());
        builder.endObject();
        return builder;
    }

}
