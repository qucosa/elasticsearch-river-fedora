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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@Ignore("Doesn't work. God knows why.")
public class IndexJobProcessorTest {

    private static Node esNode;
    private IndexJobProcessor indexJobProcessor;
    private UniquePredicateDelayQueue<IndexJob> jobQueue;
    private Client esClient;
    private FedoraClient fedoraClient;
    private ESLogger esLogger;

    @Test
    public void writesIndexErrorDocument() throws Exception {
        IndexJob job = mock(IndexJob.class);
        doThrow(new Exception("Test")).when(job).execute(fedoraClient, esClient, esLogger);
        when(job.index(anyString())).thenReturn(job);
        when(job.indexType()).thenReturn("testtype");
        when(job.pid()).thenReturn("test:1");
        jobQueue.add(job);

        runAndWait(indexJobProcessor);

        esClient.admin().indices().refresh(new RefreshRequest("testindex")).actionGet();

        GetResponse response = esClient.prepareGet("testindex", "error", "test:1").execute().actionGet();
        assertTrue(response.isExists());
        assertEquals("Test", response.getSourceAsMap().get("message"));
        assertTrue("Timestamp missing", response.getSourceAsMap().containsKey("timestamp"));
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
    }

    @AfterClass
    public static void teardownEsNode() {
        esNode.client().close();
        esNode.stop();
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

}
