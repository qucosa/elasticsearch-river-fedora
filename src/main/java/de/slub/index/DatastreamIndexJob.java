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
import com.yourmediashelf.fedora.client.request.GetDatastream;
import com.yourmediashelf.fedora.client.response.GetDatastreamResponse;
import com.yourmediashelf.fedora.generated.management.DatastreamProfile;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class DatastreamIndexJob extends IndexJob {

    public static final String ES_TYPE_NAME = "datastream";

    public DatastreamIndexJob(Type create, String pid, String dsid, int delay, TimeUnit unit) {
        super(create, pid, dsid, delay, unit);
    }

    public DatastreamIndexJob(Type type, String pid, String dsid) {
        super(type, pid, dsid);
    }

    @Override
    public String indexType() {
        return ES_TYPE_NAME;
    }

    @Override
    protected void executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) {
        try {
            client.prepareDelete(index(), indexType(), dsid()).execute().actionGet();
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
            client.prepareIndex(index(), indexType(), dsid())
                    .setSource(buildIndexObject(fedoraClient))
                    .execute().actionGet();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private XContentBuilder buildIndexObject(FedoraClient fedoraClient) throws IOException, FedoraClientException {
        GetDatastreamResponse response = (GetDatastreamResponse)
                fedoraClient.execute(new GetDatastream(pid(), dsid()));
        DatastreamProfile profile = response.getDatastreamProfile();
        return jsonBuilder().startObject()
                .field("PID", profile.getPid())
                .field("DSID", profile.getDsID())
                .field("LABEL", profile.getDsLabel())
                .field("VERSION_ID", profile.getDsVersionID())
                .field("CREATED_DATE", profile.getDsCreateDate())
                .field("STATE", profile.getDsState())
                .field("MIMETYPE", profile.getDsMIME())
                .field("CONTROL_GROUP", profile.getDsControlGroup())
                .field("SIZE", profile.getDsSize())
                .field("VERSIONABLE", (profile.getDsVersionable().equals("true")))
                .field("CHECKSUM_TYPE", profile.getDsChecksumType())
                .field("CHECKSUM", profile.getDsChecksum())
                        //.field("CONTENT", dissemination)
                .endObject();
    }

}
