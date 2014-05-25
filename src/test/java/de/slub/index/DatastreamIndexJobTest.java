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


import com.sun.jersey.api.client.ClientResponse;
import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.request.GetDatastream;
import com.yourmediashelf.fedora.client.request.GetDatastreamDissemination;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.FedoraResponseImpl;
import com.yourmediashelf.fedora.client.response.GetDatastreamResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DatastreamIndexJobTest {

    private static Node esNode;
    private static FedoraClient fedoraClient;
    private static Client esClient;
    private static ESLogger esLogger;

    @BeforeClass
    public static void setup() {
        fedoraClient = mock(FedoraClient.class);
    }

    @BeforeClass
    public static void setupEsNode() throws InterruptedException, IOException {
        esNode = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.store.fs.memory.enabled", true)
                .put("path.data", "target/es/data")
                .put("path.logs", "target/es/logs")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0"))
                .local(true).node();
        esNode.client().admin().indices().create(new CreateIndexRequest("testindex")).actionGet();
        esNode.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        esClient = esNode.client();
        esLogger = ESLoggerFactory.getLogger(DatastreamIndexJobTest.class.getSimpleName());
    }

    @AfterClass
    public static void teardownEsNode() {
        esClient.close();
        esNode.stop();
    }

    @Test
    @Ignore("Stubbing Fedora Client API doesn't work")
    public void mapsDatastreamXmlToJson() throws Exception {
        InputStream profileInputStream = this.getClass().getResourceAsStream("/response/datastreamProfile.xml");
        GetDatastreamResponse getDatastreamResponse = new GetDatastreamResponse(
                new ClientResponse(
                        200,
                        null,
                        profileInputStream,
                        null)
        );
        doReturn(getDatastreamResponse).when(fedoraClient).execute(any(GetDatastream.class));
//        when(fedoraClient.execute(any(GetDatastream.class)))
//                .thenReturn(getDatastreamResponse);

        InputStream disseminationInputStream = this.getClass().getResourceAsStream("/response/datastreamContent.xml");
        FedoraResponse getDatastreamDissemination = new FedoraResponseImpl(
                new ClientResponse(
                        200,
                        null,
                        disseminationInputStream,
                        null)
        );
        doReturn(getDatastreamDissemination).when(fedoraClient).execute(any(GetDatastreamDissemination.class));
//        when(fedoraClient.execute((any(GetDatastreamDissemination.class))))
//                .thenReturn(getDatastreamDissemination);

        DatastreamIndexJob dsIndexJob = new DatastreamIndexJob(
                IndexJob.Type.CREATE,
                "test:1",
                "ds:1");
        dsIndexJob.index("testindex");
        dsIndexJob.execute(fedoraClient, esClient, esLogger);

        esClient.admin().indices().refresh(new RefreshRequest("testindex"));

        GetResponse response = esClient.prepareGet("testindex", "datastream", "ds:1").execute().actionGet();
        assertTrue(response.isExists());

        System.out.println(response.getSourceAsString());

    }

    @After
    public void teardown() {
        reset(fedoraClient);
    }


}