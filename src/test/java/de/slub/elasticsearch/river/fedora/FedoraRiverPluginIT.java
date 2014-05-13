package de.slub.elasticsearch.river.fedora;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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

@Ignore("Integration test needs running Fedora instance")
public class FedoraRiverPluginIT {

    public static final String FEDORA_HOST = "localhost";

    @Test
    public void riverStarts() throws InterruptedException {
        GetResponse response = node.client().get(new GetRequest("_river", "fr1", "_status")).actionGet();
        assertFalse(response.getSourceAsMap().containsKey("error"));
    }

    private static Node node;

    @BeforeClass
    public static void setupEsNode() throws InterruptedException, IOException {
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "local")).local(true).node();
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
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
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
