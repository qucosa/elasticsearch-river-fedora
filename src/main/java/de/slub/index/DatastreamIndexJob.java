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
import com.yourmediashelf.fedora.client.request.GetDatastream;
import com.yourmediashelf.fedora.client.request.GetDatastreamDissemination;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.GetDatastreamResponse;
import com.yourmediashelf.fedora.generated.management.DatastreamProfile;
import org.apache.commons.io.IOUtils;
import org.apache.tika.parser.ParsingReader;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

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
    protected java.util.List<IndexJob> executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) {
        deleteErrorDocuments(client);
        return null;
    }

    @Override
    protected java.util.List<IndexJob> executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
        executeCreate(fedoraClient, client, log);
        return null;
    }

    @Override
    protected java.util.List<IndexJob> executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
        IndexResponse response = client.prepareIndex(index(), indexType(), esid())
                .setParent(pid())
                .setSource(buildIndexObject(fedoraClient))
                .execute().actionGet();
        if (response.isCreated()) {
            deleteErrorDocuments(client);
        }
        return null;
    }

    private XContentBuilder buildIndexObject(FedoraClient fedoraClient) throws Exception {
        GetDatastreamResponse response = (GetDatastreamResponse)
                fedoraClient.execute(new GetDatastream(pid(), dsid()));
        DatastreamProfile profile = response.getDatastreamProfile();
        XContentBuilder jb = jsonBuilder().startObject()
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
                .field("CHECKSUM", profile.getDsChecksum());
        indexDatastreamContent(fedoraClient, profile, jb);
        jb.endObject();
        return jb;
    }

    private void indexDatastreamContent(FedoraClient fedoraClient, DatastreamProfile profile, XContentBuilder jb) throws Exception {
        jb.startObject("CONTENT");
        URI contentURI = new URI(
                String.format("objects/%s/datastreams/%s/content",
                        profile.getPid(),
                        profile.getDsID())
        );
        jb.field("_uri", contentURI.toASCIIString());
        FedoraResponse dsResponse = fedoraClient.execute(new GetDatastreamDissemination(
                profile.getPid(), profile.getDsID()
        ));

        if (dsResponse.getStatus() != 200) {
            throw new Exception("Couldn't get datastream content for indexing. Fedora server status: " + dsResponse.getStatus());
        }

        InputStream contentInputStream = dsResponse.getEntityInputStream();
        try {
            Reader reader = new ParsingReader(contentInputStream);
            jb.field("_content", IOUtils.toString(reader));
        } finally {
            contentInputStream.close();
        }

        jb.endObject();
    }

    private void deleteErrorDocuments(Client client) {
        client.prepareDeleteByQuery(index())
                .setTypes(IndexJobProcessor.ES_ERROR_TYPE_NAME)
                .setQuery(termQuery("_id", esid()))
                .execute().actionGet();
    }

}
