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

package de.slub.util.xslt;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class JsonTransformer {

    private static JsonTransformer instance;

    private final Transformer transformer;
    private final DocumentBuilder documentBuilder;

    private JsonTransformer() throws TransformerConfigurationException, ParserConfigurationException {
        transformer = TransformerFactory.newInstance().newTransformer(
                new StreamSource(JsonTransformer.class.getResourceAsStream("/xml2json.xslt"))
        );
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }

    public static JsonTransformer getInstance() throws Exception {
        return (instance != null) ? instance :
                new JsonTransformer();
    }

    public String toJson(InputStream xml) throws IOException, SAXException, TransformerException {
        Document inputDocument = documentBuilder.parse(xml);
        StringWriter sw = new StringWriter();
        StreamResult out = new StreamResult(sw);
        transformer.transform(new DOMSource(inputDocument), out);
        return sw.toString();
    }
}
