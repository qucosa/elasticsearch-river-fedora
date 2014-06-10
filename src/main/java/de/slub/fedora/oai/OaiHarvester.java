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

    private final ESLogger logger;
    private final URL url;
    private final TimeValue interval;
    private final Client client;
    private final RiverName riverName;
    private final Queue<IndexJob> jobQueue;
    private Date lastrun;
    private String resumptionToken;
    private Date expirationDate;

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
            Date timestamp = now();
            getLastrunParameters();

            if (lastrun != null && timestamp.before(lastrun)) {
                TimeUnit.MILLISECONDS.sleep(interval.getMillis());
                continue;
            }

            URI uri = buildOaiRequestURI();
            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpClient httpClient = HttpClients.createMinimal();

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    HttpEntity httpEntity = httpResponse.getEntity();
                    if (httpEntity != null) {
                        handleXmlResult(httpEntity.getContent());
                        writeLastrun(timestamp);
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
            } finally {
                if (resumptionTokenIsPresent()) {
                    TimeUnit.SECONDS.sleep(1);
                } else {
                    TimeUnit.MILLISECONDS.sleep(interval.getMillis());
                }
            }
        }
    }

    private boolean resumptionTokenIsPresent() {
        return resumptionToken != null && !resumptionToken.isEmpty();
    }

    private Date now() {
        return Calendar.getInstance().getTime();
    }

    private void getLastrunParameters() {
        GetResponse response = client.prepareGet(riverName.name(), riverName.type(), "_last").execute().actionGet();
        if (response.isExists()) {
            Map<String, Object> src = response.getSourceAsMap();
            lastrun = getDate(src, "timestamp");
            expirationDate = getDate(src, "expiration_date");
            resumptionToken = (String) src.get("resumption_token");
        }
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

    private URI buildOaiRequestURI() throws URISyntaxException {
        UriBuilder builder = UriBuilder.fromUri(url.toURI())
                .queryParam("verb", "ListIdentifiers");

        if (isValidResumptionToken(resumptionToken, expirationDate)) {
            builder.queryParam("resumptionToken", resumptionToken);
        } else {
            builder.queryParam("metadataPrefix", "oai_dc");
            if (lastrun != null) {
                builder.queryParam("from", new SimpleDateFormat("YYYY-MM-DD").format(lastrun));
            }
        }

        return builder.build();
    }

    private boolean isValidResumptionToken(String token, Date expires) {
        return token != null
                && !token.isEmpty()
                && (expires == null || expires.after(now()));
    }

    private void writeLastrun(final Date timestamp) throws IOException {
        XContentBuilder jb = jsonBuilder().startObject();
        jb.field("timestamp", timestamp);
        if (resumptionTokenIsPresent()) {
            jb.field("resumption_token", resumptionToken);
        }
        if (expirationDate != null) {
            jb.field("expiration_date", expirationDate);
        }
        jb.endObject();
        client.prepareIndex(riverName.getName(), riverName.type(), "_last")
                .setSource(jb)
                .execute().actionGet();
    }

    private void handleXmlResult(
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
        resumptionToken = (String) xSelectResumptionToken.evaluate(document, XPathConstants.STRING);

        XPathExpression xSelectExpirationDate = xPath.compile("//resumptionToken/@expirationDate");
        String s = (String) xSelectExpirationDate.evaluate(document, XPathConstants.STRING);
        if (s == null || s.isEmpty()) {
            expirationDate = null;
        } else {
            expirationDate = DatatypeConverter.parseDateTime(s).getTime();
        }
    }

    private String getLocalIdentifier(String oaiId) {
        return oaiId.substring(oaiId.indexOf(':', "oai:".length()) + 1);
    }

}
