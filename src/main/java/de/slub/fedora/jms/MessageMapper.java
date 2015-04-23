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

package de.slub.fedora.jms;

import de.slub.index.DatastreamIndexJob;
import de.slub.index.IndexJob;
import de.slub.index.ObjectIndexJob;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.jms.Message;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static de.slub.index.IndexJob.Type.CREATE;
import static de.slub.index.IndexJob.Type.DELETE;
import static de.slub.index.IndexJob.Type.UPDATE;

public class MessageMapper {

    private static final DocumentBuilderFactory documentBuilderFactory =
            DocumentBuilderFactory.newInstance();
    private static final XPathFactory xPathFactory;
    private static final XPath xPath;

    static {
        xPathFactory = XPathFactory.newInstance();
        xPath = xPathFactory.newXPath();
    }

    public static List<IndexJob> map(Message message) throws Exception {
        ArrayList<IndexJob> indexJobs = new ArrayList<>();
        String pid = message.getStringProperty("pid");
        String dsid = extractDsId(((TextMessage) message).getText());
        String methodName = message.getStringProperty("methodName");
        switch (methodName) {
            case "ingest":
                indexJobs.add(new ObjectIndexJob(CREATE, pid, 1, TimeUnit.SECONDS));
                break;
            case "addDatastream":
                indexJobs.add(new DatastreamIndexJob(CREATE, pid, dsid, 1, TimeUnit.SECONDS));
                break;
            case "purgeObject":
                indexJobs.add(new ObjectIndexJob(DELETE, pid));
                break;
            case "purgeDatastream":
                indexJobs.add(new DatastreamIndexJob(DELETE, pid, dsid));
                break;
            case "modifyObject":
                indexJobs.add(new ObjectIndexJob(UPDATE, pid, 5, TimeUnit.SECONDS));
                break;
            case "modifyDatastreamByReference":
            case "modifyDatastreamByValue":
            case "setDatastreamState":
                indexJobs.add(new ObjectIndexJob(UPDATE, pid, dsid, 1, TimeUnit.SECONDS));
                indexJobs.add(new DatastreamIndexJob(UPDATE, pid, dsid, 5, TimeUnit.SECONDS));
                break;
        }
        return indexJobs;
    }

    private static String extractDsId(String text) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(IOUtils.toInputStream(text));

        return (String) xPath.evaluate("/entry/category[@scheme='fedora-types:dsID']/@term", document, XPathConstants.STRING);
    }

}
