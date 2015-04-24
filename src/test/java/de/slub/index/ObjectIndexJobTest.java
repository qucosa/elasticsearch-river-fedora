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
import com.yourmediashelf.fedora.client.request.FedoraRequest;
import com.yourmediashelf.fedora.client.response.GetDatastreamResponse;
import com.yourmediashelf.fedora.client.response.GetObjectProfileResponse;
import com.yourmediashelf.fedora.generated.management.DatastreamProfile;
import de.slub.rules.InMemoryElasticsearchNode;
import org.codehaus.jackson.map.util.ISO8601DateFormat;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.node.Node;
import org.junit.*;

import java.text.ParseException;

import static de.slub.index.IndexJob.Type.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ObjectIndexJobTest {

    private static final ISO8601DateFormat dateFormatParser = new ISO8601DateFormat();
    private static final ESLogger esLogger = ESLoggerFactory.getLogger("test-logger");

    @ClassRule
    public static InMemoryElasticsearchNode esNodeRule = new InMemoryElasticsearchNode();
    private Node esNode = esNodeRule.getEsNode();

    private FedoraClient fedoraClient;

    @Test
    public void executesCreateIndexDocument() throws Exception {
        ObjectIndexJob job = new ObjectIndexJob(CREATE, "test:1234");
        job.index("idx1").execute(fedoraClient, esNode.client(), esLogger);
        GetResponse response = esNode.client().get(new GetRequest("idx1", ObjectIndexJob.ES_TYPE_NAME, "test:1234")).actionGet();
        assertTrue(response.isExists());
    }

    @Test
    public void deleteErrorDocumentsWhenEverythingWentFine() throws Exception {
        esNode.client().prepareIndex("idx1", "error", "test:1234")
                .setSource(jsonBuilder().startObject()
                        .field("PID", "test:1234")
                        .endObject())
                .execute().actionGet();

        ObjectIndexJob job = new ObjectIndexJob(CREATE, "test:1234");
        job.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        GetResponse response = esNode.client().get(new GetRequest("idx1", "error", "test:1234")).actionGet();
        assertFalse(response.isExists());
    }

    @Test
    @Ignore("Fedora datastream response not completely mocked")
    public void deleteIndexJobRemovesDocumentFromIndex() throws Exception {
        ObjectIndexJob job1 = new ObjectIndexJob(CREATE, "test:1234");
        job1.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        ObjectIndexJob job2 = new ObjectIndexJob(DELETE, "test:1234");
        job2.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        GetResponse response = esNode.client().get(new GetRequest("idx1", ObjectIndexJob.ES_TYPE_NAME, "test:1234")).actionGet();
        assertFalse(response.isExists());
    }

    @Test
    @Ignore("Fedora datastream response not completely mocked")
    public void deletesDatastreamDocumentsFromIndex() throws Exception {
        ObjectIndexJob job1 = new ObjectIndexJob(CREATE, "test:1234");
        job1.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        DatastreamIndexJob job2 = new DatastreamIndexJob(CREATE, "test:1234", "DS");
        job2.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        ObjectIndexJob job3 = new ObjectIndexJob(DELETE, "test:1234");
        job3.index("idx1").execute(fedoraClient, esNode.client(), esLogger);

        GetResponse response = esNode.client().get(new GetRequest("idx1", DatastreamIndexJob.ES_TYPE_NAME, "test:1234:DS")).actionGet();
        assertFalse(response.isExists());
    }

    @Test
    public void updatesIndexDocument() throws Exception {
        esNode.client().prepareIndex("idx1", ObjectIndexJob.ES_TYPE_NAME, "test:1234-2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("should", "disappear")
                        .endObject())
                .execute().actionGet();

        ObjectIndexJob job1 = new ObjectIndexJob(UPDATE, "test:1234-2");
        job1.index("idx1").execute(fedoraClient, esNode.client(), esLogger);
        GetResponse response = esNode.client().get(new GetRequest("idx1", ObjectIndexJob.ES_TYPE_NAME, "test:1234-2")).actionGet();
        assertTrue(response.getSourceAsMap().containsKey("PID"));
        assertEquals(2, response.getVersion());
    }

    @Before
    public void mockFedoraClient() throws FedoraClientException, ParseException {
        fedoraClient = mock(FedoraClient.class);

        GetObjectProfileResponse mockGetObjectProfileResponse = mock(GetObjectProfileResponse.class);
        when(mockGetObjectProfileResponse.getPid()).thenReturn("test:1234");
        when(mockGetObjectProfileResponse.getState()).thenReturn("A");
        when(mockGetObjectProfileResponse.getCreateDate()).thenReturn(dateFormatParser.parse("2014-05-07T18:33:33.996Z"));
        when(mockGetObjectProfileResponse.getLastModifiedDate()).thenReturn(dateFormatParser.parse("2014-05-07T18:35:29.636Z"));
        when(mockGetObjectProfileResponse.getLabel()).thenReturn("The label");
        when(mockGetObjectProfileResponse.getOwnerId()).thenReturn("SLUB");

        GetDatastreamResponse mockDatastreamResponse = mock(GetDatastreamResponse.class);
        DatastreamProfile datastreamProfile = new DatastreamProfile();
        datastreamProfile.setPid("test:1234");
        datastreamProfile.setDsID("DS");
        datastreamProfile.setDsVersionable("true");
        when(mockDatastreamResponse.getDatastreamProfile()).thenReturn(datastreamProfile);

        when(fedoraClient.execute(any(FedoraRequest.class)))
                .thenReturn(mockGetObjectProfileResponse)
                .thenReturn(mockDatastreamResponse);
    }

    @After
    public void resetMockFedoraClient() {
        reset(fedoraClient);
    }


}
