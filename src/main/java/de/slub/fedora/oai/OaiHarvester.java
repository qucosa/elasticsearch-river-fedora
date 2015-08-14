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
import java.io.PrintWriter;
import java.io.StringWriter;
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

    private static final OaiRunResult EMPTY_OAI_RUN_RESULT = new OaiRunResult();
    private static final SimpleDateFormat FCREPO3_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
    private final Client client;
    private final TimeValue interval;
    private final Queue<IndexJob> jobQueue;
    private final ESLogger logger;
    private final RiverName riverName;
    private final URI uri;

    protected OaiHarvester(
            URL harvestingUrl,
            TimeValue pollInterval,
            Client esClient,
            RiverName riverName,
            Queue<IndexJob> indexJobQueue,
            ESLogger logger) throws URISyntaxException {

        this.uri = harvestingUrl.toURI();
        this.interval = pollInterval;
        this.client = esClient;
        this.jobQueue = indexJobQueue;
        this.riverName = riverName;
        this.logger = logger;

        this.logger.info("Harvesting URL: {} every {}", this.uri.toASCIIString(), this.interval.format());
    }

    @Override
    public void run() {
        try {
            harvestLoop();
        } catch (Exception e) {
            logger.error(ensureMessage(e));
        }
    }

    private void harvestLoop() {
        while (isRunning()) {
            if (waitForNextRun(getLastrunParameters())) {
                // update last run info in case it has been changed while waiting
                final OaiRunResult lastrun = getLastrunParameters();
                final OaiRunResult newlastrun = harvest(lastrun);
                writeLastrun(newlastrun);
            }
        }
    }

    private boolean waitForNextRun(OaiRunResult lastrun) {
        Date start = now();
        TimeValue waitTime = interval;

        if (lastrun.isInFuture(start)) {
            long delta = lastrun.getTimestamp().getTime() - start.getTime();
            waitTime = TimeValue.timeValueMillis(delta);
        } else if (lastrun.hasResumptionToken()) {
            waitTime = TimeValue.timeValueSeconds(1);
        }

        try {
            TimeUnit.MILLISECONDS.sleep(waitTime.getMillis());
            return true;
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for next OAI run: {}", e.getMessage());
            return false;
        }
    }

    private OaiRunResult harvest(OaiRunResult lastRun) {
        Date timeOfRun = now();
        URI uri = buildOaiRequestURI(timeOfRun, lastRun);

        final Date lastRunTimestamp = lastRun.getTimestamp();
        if (lastRunTimestamp != null) logger.debug("Last OAI run was at {}", lastRunTimestamp);

        logger.debug("Requesting {}", uri.toASCIIString());

        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpClient httpClient = HttpClients.createMinimal();

        OaiRunResult result = EMPTY_OAI_RUN_RESULT;
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpEntity != null) {
                    result = handleXmlResult(httpEntity.getContent(), timeOfRun);
                } else {
                    logger.warn("Got empty response from OAI service.");
                }
            } else {
                logger.error("Unexpected OAI service response: {} {}",
                        httpResponse.getStatusLine().getStatusCode(),
                        httpResponse.getStatusLine().getReasonPhrase());
            }
        } catch (Exception ex) {
            logger.error(ensureMessage(ex));
        }
        return result;
    }

    private String ensureMessage(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            message = sw.toString();
        }
        return message;
    }

    private Date now() {
        return Calendar.getInstance().getTime();
    }

    private OaiRunResult getLastrunParameters() {
        OaiRunResult result = EMPTY_OAI_RUN_RESULT;
        try {
            GetResponse response = client.prepareGet(riverName.name(), riverName.type(), "_last").execute().actionGet();
            if (response.isExists()) {
                Map<String, Object> src = response.getSourceAsMap();
                result = new OaiRunResult(
                        getDate(src, "timestamp"),
                        getDate(src, "expiration_date"),
                        (String) src.get("resumption_token"));
            }
        } catch (Exception _) {
            logger.warn("Error parsing the '_last' river run document. Assuming there was no run...");
        }
        return result;
    }

    private Date getDate(Map<String, Object> src, String param) {
        if (src.containsKey(param)) {
            String s = (String) src.get(param);
            if (s != null && !s.isEmpty()) {
                return DatatypeConverter.parseDateTime(s).getTime();
            }
        }
        return null;
    }

    private URI buildOaiRequestURI(Date now, OaiRunResult lastrun) {
        UriBuilder builder = UriBuilder.fromUri(uri)
                .queryParam("verb", "ListIdentifiers");

        if (lastrun.isValidResumptionToken(now)) {
            builder.queryParam("resumptionToken", lastrun.getResumptionToken());
        } else {
            builder.queryParam("metadataPrefix", "oai_dc");
            if (lastrun.hasTimestamp()) {
                builder.queryParam("from", FCREPO3_TIMESTAMP_FORMAT.format(lastrun.getTimestamp()));
            }
        }

        return builder.build();
    }

    private void writeLastrun(OaiRunResult runResult) {
        try {
            XContentBuilder jb = jsonBuilder().startObject();
            jb.field("timestamp", runResult.getTimestamp());
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
        } catch (IOException e) {
            logger.error("Cannot write last run information: {}", e.getMessage());
        }
    }

    private OaiRunResult handleXmlResult(
            InputStream content, Date timeOfRun)
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
        logger.debug("{} elements in OAI result", nodes.getLength());
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            String localIdentifier = getLocalIdentifier(n.getTextContent());
            boolean added = jobQueue.add(
                    new ObjectIndexJob(IndexJob.Type.CREATE,
                            localIdentifier));
            if (added) logger.debug("Added {} to job queue", localIdentifier);
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

        return new OaiRunResult(timeOfRun, expirationDate, resumptionToken);
    }

    private String getLocalIdentifier(String oaiId) {
        return oaiId.substring(oaiId.indexOf(':', "oai:".length()) + 1);
    }

}
