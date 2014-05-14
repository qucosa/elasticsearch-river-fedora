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

package de.slub.elasticsearch.river.fedora;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore("Integration test requires a running Fedora instance")
public class FedoraRiverPluginIT {

    public static final String FEDORA_HOST = "localhost";

    @Test
    public void riverStarts() throws InterruptedException {
        GetResponse response = node.client().get(new GetRequest("_river", "fr1", "_status")).actionGet();
        assertFalse(response.getSourceAsMap().containsKey("error"));
    }

    @Test
    public void riverInitializesObjectIndex() {
        IndicesExistsResponse response = node.client().admin().indices().exists(
                new IndicesExistsRequest("fedora")
        ).actionGet();
        assertTrue(response.isExists());
    }

    private static Node node;

    @BeforeClass
    public static void setupEsNode() throws InterruptedException, IOException {
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("gateway.indexType", "local")
                .put("index.store.type", "memory")
                .put("index.store.fs.memory.enabled", true)
                .put("path.data", "target/es/data")
                .put("path.logs", "target/es/logs"))
                .local(true).node();
        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute();
    }

    @AfterClass
    public static void teardownEsNode() {
        node.client().close();
        node.stop();
    }


    @Before
    public void setupRiver() throws IOException, InterruptedException {
        node.client().prepareIndex("_river", "fr1", "_meta").setSource(
                jsonBuilder().startObject()
                        .field("type", "fedora-river")
                        .field("indexName", "fedora")
                        .startObject("jms")
                        .field("brokerUrl", "tcp://" + FEDORA_HOST + ":61616").endObject()
                        .startObject("fedora")
                        .field("url", "http://" + FEDORA_HOST + ":8080/fedora")
                        .field("username", "fedoraAdmin")
                        .field("password", "fedoraAdmin").endObject()
                        .endObject()
        ).execute().actionGet();
        node.client().admin().indices().refresh(
                new RefreshRequest()
        );
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    @After
    public void teardownRiver() {
        try {
            node.client().admin().indices().delete(new DeleteIndexRequest("_river")).actionGet();
        } catch (IndexMissingException e) {
            // Index does not exist... Fine
        }
    }

}
