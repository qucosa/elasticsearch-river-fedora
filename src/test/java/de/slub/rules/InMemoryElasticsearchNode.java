/*
 * Copyright 2015 SLUB Dresden
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

package de.slub.rules;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.rules.ExternalResource;

import java.io.IOException;

public class InMemoryElasticsearchNode extends ExternalResource {

    private Node esNode;

    public Node getEsNode() {
        return esNode;
    }

    @Override
    public void before() throws InterruptedException, IOException {
        esNode = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.store.fs.memory.enabled", true)
                .put("path.data", "target/es/data")
                .put("path.logs", "target/es/logs")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0"))
                .local(true).node();
        esNode.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
    }

    @Override
    public void after() {
        esNode.client().close();
        esNode.stop();
    }

}
