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

package de.slub.fedora.oai;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.slub.index.IndexJob;
import de.slub.index.ObjectIndexJob;
import de.slub.rules.InMemoryElasticsearchNode;
import de.slub.util.TerminateableRunnable;
import de.slub.util.concurrent.UniquePredicateDelayQueue;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.RiverName;
import org.junit.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Calendar;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

public class OaiHarvesterTestIT {
    @ClassRule
    public static InMemoryElasticsearchNode esNodeRule = new InMemoryElasticsearchNode();

    private static final String OAI_LIST_RECORDS_XML = "/oai/listIdentifiers.xml";
    private static final String OAI_RESUMPTION_TOKEN_XML = "/oai/resumptionToken.xml";
    private static final String OAI_EMPTY_RESUMPTION_TOKEN_XML = "/oai/emptyResumptionToken.xml";
    private Node esNode = esNodeRule.getEsNode();
    private HttpServer httpServer;
    private EmbeddedHttpHandler embeddedHttpHandler;
    private OaiHarvester oaiHarvester;
    private Queue<IndexJob> jobQueue;

    @Test
    public void createdObjectIndexJobForListedRecord() throws Exception {
        embeddedHttpHandler.resourcePath = OAI_LIST_RECORDS_XML;
        runAndWait(oaiHarvester);

        assertTrue(jobQueue.contains(
                new ObjectIndexJob(IndexJob.Type.CREATE, "qucosa:1044")));
    }

    @Test
    public void writesLastrunTimestamp() throws Exception {
        embeddedHttpHandler.resourcePath = OAI_LIST_RECORDS_XML;
        runAndWait(oaiHarvester);

        GetResponse response = esNode.client().get(
                new GetRequest("_river", "fedora", "_last")).actionGet();
        assertTrue("Last run index document is not present", response.isExists());
        assertTrue("Last run index document doesn't contain timestamp field",
                response.getSourceAsMap().containsKey("timestamp"));
    }

    @Test
    public void usesFromQueryWhenLastrunTimestampPresent() throws Exception {
        embeddedHttpHandler.resourcePath = OAI_LIST_RECORDS_XML;
        esNode.client().prepareIndex("_river", "fedora", "_last")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("timestamp", Calendar.getInstance().getTime())
                        .endObject())
                .execute().actionGet();

        runAndWait(oaiHarvester);

        assertTrue("Missing from parameter in OAI query",
                embeddedHttpHandler.lastRequestUri.getQuery().contains("from="));
    }

    @Test
    public void doesNothingWhenLastrunIsInFuture() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, 1);

        esNode.client().prepareIndex("_river", "fedora", "_last")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("timestamp", cal.getTime())
                        .endObject())
                .execute().actionGet();

        embeddedHttpHandler.resourcePath = OAI_LIST_RECORDS_XML;
        runAndWait(oaiHarvester);

        assertNull("Last run is in future. Should not harvest.",
                embeddedHttpHandler.lastRequestUri);
    }

    @Test
    public void setsResumptionTokenAndExpirationDate() throws Exception {
        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        GetResponse response = esNode.client().get(
                new GetRequest("_river", "fedora", "_last")).actionGet();
        assertTrue("Last run index document is not present", response.isExists());
        assertTrue("Last run index document doesn't contain resumption_token field",
                response.getSourceAsMap().containsKey("resumption_token"));
        assertTrue("Last run index document doesn't contain expiration_date field",
                response.getSourceAsMap().containsKey("expiration_date"));
        assertEquals("140225245500000", response.getSourceAsMap().get("resumption_token"));
        assertEquals("2014-06-09T18:34:15.000Z", response.getSourceAsMap().get("expiration_date"));
    }

    @Test
    public void usesResumptionToken() throws Exception {
        esNode.client().prepareIndex("_river", "fedora", "_last")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("resumption_token", "xyz1234")
                        .endObject())
                .execute().actionGet();

        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        assertFalse("Query parameter metadataPrefix is not allowed when using resumptionToken",
                embeddedHttpHandler.lastRequestUri.getQuery().contains("metadataPrefix"));
        assertTrue("Missing resumptionToken parameter in OAI query",
                embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken=xyz1234"));
    }

    @Test
    public void rejectsResumptionTokenIfOutdated() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);

        esNode.client().prepareIndex("_river", "fedora", "_last")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("resumption_token", "xyz1234")
                        .field("expiration_date", cal.getTime())
                        .endObject())
                .execute().actionGet();

        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        assertFalse("Outdated resumptionToken should not be used",
                embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken="));
    }

    @Test
    public void emptiesResumptionTokenInIndexDocument() throws Exception {
        esNode.client().prepareIndex("_river", "fedora", "_last")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("resumption_token", "xyz1234")
                        .endObject())
                .execute().actionGet();

        embeddedHttpHandler.resourcePath = OAI_EMPTY_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        GetResponse response = esNode.client().get(
                new GetRequest("_river", "fedora", "_last")).actionGet();
        assertFalse("Last run index document contains resumption_token field",
                response.getSourceAsMap().containsKey("resumption_token"));
    }

    @Before
    public void createOaiHarvester() throws Exception {
        jobQueue = new UniquePredicateDelayQueue<>();
        oaiHarvester = new OaiHarvesterBuilder()
                .url(new URL("http://localhost:8000/fedora/oai"))
                .esClient(esNode.client())
                .interval(new TimeValue(1, TimeUnit.SECONDS))
                .riverName(new RiverName("fedora", "_river"))
                .indexJobQueue(jobQueue)
                .logger(ESLoggerFactory.getLogger(this.getClass().getName())).build();
    }

    private void runAndWait(TerminateableRunnable runnable) throws InterruptedException {
        Thread thread = new Thread(runnable);
        thread.start();
        TimeUnit.MILLISECONDS.sleep(1000);
        runnable.terminate();
        thread.join();
    }

    @Before
    public void setupHttpServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
        embeddedHttpHandler = new EmbeddedHttpHandler();
        httpServer.createContext("/fedora/oai", embeddedHttpHandler);
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();
    }

    @After
    public void stopHttpServer() {
        httpServer.stop(1);
    }

    @Before
    public void setupRiverLastrun() throws IOException, InterruptedException {
        esNode.client().admin().indices().create(new CreateIndexRequest("_river"));
        esNode.client().admin().indices().refresh(new RefreshRequest());
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

    class EmbeddedHttpHandler implements HttpHandler {

        public URI lastRequestUri;
        public String resourcePath;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            lastRequestUri = exchange.getRequestURI();
            exchange.sendResponseHeaders(200, 0);
            IOUtils.copy(
                    this.getClass().getResourceAsStream(resourcePath),
                    exchange.getResponseBody());
            exchange.getResponseBody().close();
        }
    }

}
