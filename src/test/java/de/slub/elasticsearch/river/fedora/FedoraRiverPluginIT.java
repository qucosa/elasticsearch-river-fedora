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

import de.slub.rules.InMemoryElasticsearchNode;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore("Integration test requires a running Fedora instance")
public class FedoraRiverPluginIT {

    @ClassRule
    public static InMemoryElasticsearchNode esNodeRule = new InMemoryElasticsearchNode();
    private Node esNode = esNodeRule.getEsNode();

    @Test
    public void riverStarts() throws InterruptedException {
        GetResponse response = esNode.client().get(new GetRequest("_river", "fr1", "_status")).actionGet();
        assertFalse(response.getSourceAsMap().containsKey("error"));
    }

    @Test
    public void riverInitializesObjectIndex() {
        IndicesExistsResponse response = esNode.client().admin().indices().exists(
                new IndicesExistsRequest("fedora")
        ).actionGet();
        assertTrue(response.isExists());
    }

    @Before
    public void setupRiver() throws IOException, InterruptedException {
        esNode.client().prepareIndex("_river", "fr1", "_meta")
                .setSource(
                        IOUtils.toString(this.getClass().getResourceAsStream("/config/fedora-river-config.json")))
                .execute().actionGet();
        esNode.client().admin().indices().refresh(
                new RefreshRequest()
        );
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    @After
    public void teardownRiver() {
        try {
            esNode.client().admin().indices().delete(new DeleteIndexRequest("_river")).actionGet();
        } catch (IndexMissingException e) {
            // Index does not exist... Fine
        }
    }

}
