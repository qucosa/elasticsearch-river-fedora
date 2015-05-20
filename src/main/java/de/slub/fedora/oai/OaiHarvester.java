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

import de.slub.index.IndexJob;
import de.slub.index.ObjectIndexJob;
import de.slub.util.TerminateableRunnable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.RiverName;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class OaiHarvester extends TerminateableRunnable {

    public static final OaiRunResult EMPTY_OAI_RUN_RESULT = new OaiRunResult();
    private final Client client;
    private final TimeValue interval;
    private final Queue<IndexJob> jobQueue;
    private final ESLogger logger;
    private final RiverName riverName;
    private final URL url;

    protected OaiHarvester(
            URL harvestingUrl,
            TimeValue pollInterval,
            Client esClient,
            RiverName riverName,
            Queue<IndexJob> indexJobQueue,
            ESLogger logger)
            throws
            MalformedURLException,
            URISyntaxException {

        url = harvestingUrl;
        interval = pollInterval;
        client = esClient;
        jobQueue = indexJobQueue;
        this.riverName = riverName;
        this.logger = logger;

        logger.info("Harvesting URL: {} every {}", url.toExternalForm(), interval.format());
    }

    @Override
    public void run() {
        try {
            harvestLoop();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void harvestLoop() throws URISyntaxException, InterruptedException {
        while (isRunning()) {
            OaiRunResult lastrun = getLastrunParameters();
            waitForNextRun(lastrun);
            harvest(lastrun);
        }
    }

    private void waitForNextRun(OaiRunResult lastrun) throws InterruptedException {
        Date start = now();
        TimeValue waitTime = interval;
        if (lastrun.isInFuture(start)) {
            long delta = lastrun.getTimestamp().getTime() - start.getTime();
            waitTime = TimeValue.timeValueMillis(delta);
        } else if (lastrun.hasResumptionToken()) {
            waitTime = TimeValue.timeValueSeconds(1);
        }
        TimeUnit.MILLISECONDS.sleep(waitTime.getMillis());
    }

    private void harvest(OaiRunResult lastRun) throws URISyntaxException, InterruptedException {
        Date timeOfRun = now();
        URI uri = buildOaiRequestURI(timeOfRun, lastRun);
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpClient httpClient = HttpClients.createMinimal();

        try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpEntity != null) {
                    OaiRunResult runResult = handleXmlResult(httpEntity.getContent());
                    writeLastrun(timeOfRun, runResult);
                } else {
                    logger.warn("Got empty response from OAI service.");
                }
            } else {
                logger.error("Unexpected OAI service response: {} {}",
                        httpResponse.getStatusLine().getStatusCode(),
                        httpResponse.getStatusLine().getReasonPhrase());
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    private Date now() {
        return Calendar.getInstance().getTime();
    }

    private OaiRunResult getLastrunParameters() {
        GetResponse response = client.prepareGet(riverName.name(), riverName.type(), "_last").execute().actionGet();
        if (response.isExists()) {
            Map<String, Object> src = response.getSourceAsMap();
            return new OaiRunResult(
                    getDate(src, "timestamp"),
                    getDate(src, "expiration_date"),
                    (String) src.get("resumption_token"));
        }
        return EMPTY_OAI_RUN_RESULT;
    }

    private Date getDate(Map<String, Object> src, String param) {
        if (src.containsKey(param)) {
            String s = (String) src.get(param);
            if (!s.isEmpty()) {
                return DatatypeConverter.parseDateTime(s).getTime();
            }
        }
        return null;
    }

    private URI buildOaiRequestURI(Date now, OaiRunResult lastrun) throws URISyntaxException {
        UriBuilder builder = UriBuilder.fromUri(url.toURI())
                .queryParam("verb", "ListIdentifiers");

        if (lastrun.isValidResumptionToken(now)) {
            builder.queryParam("resumptionToken", lastrun.getResumptionToken());
        } else {
            builder.queryParam("metadataPrefix", "oai_dc");
            if (lastrun.hasTimestamp()) {
                builder.queryParam("from", new SimpleDateFormat("YYYY-MM-DD").format(lastrun.getTimestamp()));
            }
        }

        return builder.build();
    }

    private void writeLastrun(final Date timestamp, OaiRunResult runResult) throws IOException {
        XContentBuilder jb = jsonBuilder().startObject();
        jb.field("timestamp", timestamp);
        if (runResult.hasResumptionToken()) {
            jb.field("resumption_token", runResult.getResumptionToken());
        }
        if (runResult.getExpirationDate() != null) {
            jb.field("expiration_date", runResult.getExpirationDate());
        }
        jb.endObject();
        client.prepareIndex(riverName.getName(), riverName.type(), "_last")
                .setSource(jb)
                .execute().actionGet();
    }

    private OaiRunResult handleXmlResult(
            InputStream content)
            throws
            ParserConfigurationException,
            IOException,
            SAXException,
            XPathExpressionException {

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        Document document = documentBuilderFactory.newDocumentBuilder().parse(content);

        XPath xPath = XPathFactory.newInstance().newXPath();

        XPathExpression xSelectIdentifier = xPath.compile("//header/identifier");
        NodeList nodes = (NodeList) xSelectIdentifier.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            jobQueue.add(
                    new ObjectIndexJob(IndexJob.Type.CREATE,
                            getLocalIdentifier(n.getTextContent())));
        }

        XPathExpression xSelectResumptionToken = xPath.compile("//resumptionToken");
        String resumptionToken = (String) xSelectResumptionToken.evaluate(document, XPathConstants.STRING);

        XPathExpression xSelectExpirationDate = xPath.compile("//resumptionToken/@expirationDate");
        String s = (String) xSelectExpirationDate.evaluate(document, XPathConstants.STRING);
        Date expirationDate;
        if (s == null || s.isEmpty()) {
            expirationDate = null;
        } else {
            expirationDate = DatatypeConverter.parseDateTime(s).getTime();
        }

        return new OaiRunResult(null, expirationDate, resumptionToken);
    }

    private String getLocalIdentifier(String oaiId) {
        return oaiId.substring(oaiId.indexOf(':', "oai:".length()) + 1);
    }

}
