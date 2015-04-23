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
import de.slub.util.TerminateableRunnable;
import de.slub.util.concurrent.UniquePredicateDelayQueue;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

public class IndexJobProcessorTest {

    private static Node esNode;
    private IndexJobProcessor indexJobProcessor;
    private UniquePredicateDelayQueue<IndexJob> jobQueue;
    private Client esClient;
    private FedoraClient fedoraClient;
    private ESLogger esLogger;

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
    }

    @AfterClass
    public static void teardownEsNode() {
        esNode.client().close();
        esNode.stop();
    }

    @Test
    public void writesIndexErrorDocument() throws Exception {
        jobQueue.add(new CrashingJob(IndexJob.Type.CREATE, "test:1"));

        runAndWait(indexJobProcessor);

        esClient.admin().indices().refresh(new RefreshRequest("testindex")).actionGet();

        GetResponse response = esClient.prepareGet("testindex", "error", "test:1").execute().actionGet();
        assertTrue(response.isExists());
        assertTrue(response.getSourceAsMap().get("message").toString().contains("crashed"));
        assertTrue("Timestamp missing", response.getSourceAsMap().containsKey("timestamp"));
    }

    @Before
    public void setup() {
        jobQueue = new UniquePredicateDelayQueue<>();
        esLogger = ESLoggerFactory.getRootLogger();
        esClient = esNode.client();
        fedoraClient = mock(FedoraClient.class);
        indexJobProcessor = new IndexJobProcessor(
                jobQueue,
                "testindex",
                esClient,
                fedoraClient,
                esLogger);
    }

    @After
    public void teardown() {
        reset(fedoraClient);
        jobQueue.clear();
    }

    private void runAndWait(TerminateableRunnable runnable) throws InterruptedException {
        Thread thread = new Thread(runnable);
        thread.start();
        TimeUnit.SECONDS.sleep(1);
        runnable.terminate();
        thread.join();
    }

    private class CrashingJob extends IndexJob {
        private final Exception EXCEPTION = new Exception(this + " crashed");

        public CrashingJob(Type type, String pid) {
            super(type, pid);
        }

        @Override
        protected List<IndexJob> executeDelete(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
            throw EXCEPTION;
        }

        @Override
        protected List<IndexJob> executeUpdate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
            throw EXCEPTION;
        }

        @Override
        protected List<IndexJob> executeCreate(FedoraClient fedoraClient, Client client, ESLogger log) throws Exception {
            throw EXCEPTION;
        }
    }
}
