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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class OaiHarvesterBuilder {

    public static final String DEFAULT_URL = "http://localhost:8080/fedora/oai";
    public static final TimeValue DEFAULT_INTERVAL = new TimeValue(5, TimeUnit.MINUTES);

    private Client esClient;
    private RiverName riverName;
    private Queue<IndexJob> indexJobQueue;
    private ESLogger logger;
    private URL url;
    private TimeValue interval;

    public OaiHarvester build() throws MalformedURLException, URISyntaxException {
        return new OaiHarvester(
                (url == null) ? new URL(DEFAULT_URL) : url,
                (interval == null) ? DEFAULT_INTERVAL : interval,
                esClient,
                riverName,
                indexJobQueue,
                logger);
    }

    public OaiHarvesterBuilder settings(Map<String, Object> oaiSettings)
            throws MalformedURLException {

        if (oaiSettings.containsKey("poll_interval")) {
            interval = TimeValue.parseTimeValue(String.valueOf(oaiSettings.get("poll_interval")), DEFAULT_INTERVAL);
        }
        if (oaiSettings.containsKey("url")) {
            url = new URL(XContentMapValues.nodeStringValue(oaiSettings.get("url"), DEFAULT_URL));
        }
        return this;
    }

    public OaiHarvesterBuilder esClient(Client esClient) {
        this.esClient = esClient;
        return this;
    }

    public OaiHarvesterBuilder riverName(RiverName riverName) {
        this.riverName = riverName;
        return this;
    }

    public OaiHarvesterBuilder indexJobQueue(Queue<IndexJob> indexJobQueue) {
        this.indexJobQueue = indexJobQueue;
        return this;
    }

    public OaiHarvesterBuilder logger(ESLogger logger) {
        this.logger = logger;
        return this;
    }

    public OaiHarvesterBuilder url(URL url) {
        this.url = url;
        return this;
    }

    public OaiHarvesterBuilder interval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

}
