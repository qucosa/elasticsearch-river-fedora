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
import org.codehaus.jackson.map.util.ISO8601DateFormat;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.*;

import java.io.IOException;
import java.text.ParseException;

import static de.slub.index.IndexJob.Type.CREATE;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ObjectIndexJobTest {

    private static final ISO8601DateFormat dateFormatParser = new ISO8601DateFormat();
    private static final ESLogger esLogger = ESLoggerFactory.getLogger("test-logger");
    private static Node esNode;
    private FedoraClient fedoraClient;

    @Test
    public void executesCreateIndexDocument() throws FedoraClientException {
        ObjectIndexJob job = new ObjectIndexJob(CREATE, "test:1234");
        job.index("idx1")
                .indexType("type1")
                .execute(fedoraClient, esNode.client(), esLogger);
        GetResponse response = esNode.client().get(new GetRequest("idx1", "type1", "test:1234")).actionGet();
        assertTrue(response.isExists());
    }

    @Before
    public void mockFedoraClient() throws FedoraClientException, ParseException {
        fedoraClient = mock(FedoraClient.class);
        GetObjectProfileResponse mockObjectProfile = mock(GetObjectProfileResponse.class);
        when(mockObjectProfile.getPid()).thenReturn("test:1234");
        when(mockObjectProfile.getState()).thenReturn("A");
        when(mockObjectProfile.getCreateDate()).thenReturn(dateFormatParser.parse("2014-05-07T18:33:33.996Z"));
        when(mockObjectProfile.getLastModifiedDate()).thenReturn(dateFormatParser.parse("2014-05-07T18:35:29.636Z"));
        when(mockObjectProfile.getLabel()).thenReturn("The label");
        when(mockObjectProfile.getOwnerId()).thenReturn("SLUB");
        when(fedoraClient.execute(any(GetObjectProfile.class)))
                .thenReturn(mockObjectProfile);
    }

    @After
    public void resetMockFedoraClient() {
        reset(fedoraClient);
    }

    @BeforeClass
    public static void setupEsNode() throws InterruptedException, IOException {
        esNode = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("gateway.indexType", "local")
                .put("path.data", "target/es/data")
                .put("path.logs", "target/es/logs"))
                .local(true).node();
        esNode.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute();
    }

    @AfterClass
    public static void teardownEsNode() {
        esNode.client().close();
        esNode.stop();
    }


}
