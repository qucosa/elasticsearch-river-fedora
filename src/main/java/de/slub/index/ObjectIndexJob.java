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
import com.yourmediashelf.fedora.client.request.GetDatastreams;
import com.yourmediashelf.fedora.client.request.GetDissemination;
import com.yourmediashelf.fedora.client.request.GetObjectProfile;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.GetDatastreamsResponse;
import com.yourmediashelf.fedora.client.response.GetObjectProfileResponse;
import com.yourmediashelf.fedora.generated.management.DatastreamProfile;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

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
    protected List<IndexJob> executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) throws IOException {
        client.prepareDeleteByQuery(index())
                .setTypes(ES_TYPE_NAME, DatastreamIndexJob.ES_TYPE_NAME, IndexJobProcessor.ES_ERROR_TYPE_NAME)
                .setQuery(termQuery("PID", pid()))
                .execute().actionGet();
        return null;
    }

    @Override
    protected List<IndexJob> executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
        executeCreate(fedoraClient, client, log);
        return null;
    }

    @Override
    protected List<IndexJob> executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
        client.prepareIndex(index(), indexType(), esid())
                .setSource(buildIndexObject(fedoraClient))
                .execute().actionGet();

        List<IndexJob> datastreamIndexJobs = new ArrayList<>();

        try {
            GetDatastreamsResponse getDatastreamsResponse =
                    (GetDatastreamsResponse) fedoraClient.execute(new GetDatastreams(pid()));
            Iterator it = getDatastreamsResponse.getDatastreamProfiles().iterator();
            while (it.hasNext()) {
                DatastreamProfile dp = (DatastreamProfile) it.next();
                datastreamIndexJobs.add(
                        new DatastreamIndexJob(Type.CREATE, dp.getPid(), dp.getDsID()));
            }
        } catch (Exception ex) {
            log.error("Couldn't generate datastream index jobs for {}. Reason: {}",
                    pid(), ex.getMessage());
        }

        return datastreamIndexJobs;
    }

    private XContentBuilder buildIndexObject(FedoraClient fedoraClient) throws Exception {
        GetObjectProfileResponse profileResponse = (GetObjectProfileResponse)
                fedoraClient.execute(new GetObjectProfile(pid()));
        XContentBuilder builder = jsonBuilder().startObject()
                .field("PID", profileResponse.getPid())
                .field("STATE", profileResponse.getState())
                .field("LABEL", profileResponse.getLabel())
                .field("OWNER_ID", profileResponse.getOwnerId())
                .field("CREATED_DATE", profileResponse.getCreateDate())
                .field("LAST_MODIFIED_DATE", profileResponse.getLastModifiedDate());
        addDisseminationResult(builder, fedoraClient);
        builder.endObject();
        return builder;
    }

    private void addDisseminationResult(XContentBuilder builder, FedoraClient fedoraClient)
            throws Exception {
        try {
            if (sdefPid() == null || method() == null || sdefPid().isEmpty() || method().isEmpty()) {
                return;
            }

            FedoraResponse disseminationResult =
                    fedoraClient.execute(new GetDissemination(pid(), sdefPid(), method()));

            if (disseminationResult.getStatus() == 200) {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                        .createParser(disseminationResult.getEntityInputStream());
                builder.startObject("_dissemination")
                        .field("_sdef_pid", sdefPid())
                        .field("_method", method())
                        .field("_content")
                        .copyCurrentStructure(parser)
                        .endObject();
            }
        } catch (FedoraClientException fex) {
            throw new Exception(
                    String.format("Could not obtain object dissemination with method %s/%s: %s", sdefPid(), method(), fex.getMessage()));
        } catch (IOException ioex) {
            throw new Exception(
                    String.format("Failed parsing dissemination: %s", ioex.getMessage()));
        }
    }

}
