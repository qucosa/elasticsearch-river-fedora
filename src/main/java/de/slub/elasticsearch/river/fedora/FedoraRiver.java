/*
 * Copyright 2014 SLUB Dresden
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package de.slub.elasticsearch.river.fedora;

import de.slub.elasticsearch.river.fedora.de.slub.jms.fedora.APIMConsumer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.net.URI;
import java.net.URISyntaxException;

public class FedoraRiver extends AbstractRiverComponent implements River {

    private final APIMConsumer apimConsumer;
    private final Thread apimConsumerThread;

    @Inject
    protected FedoraRiver(RiverName riverName, RiverSettings settings) throws URISyntaxException {
        super(riverName, settings);

        String brokerUrl = XContentMapValues.nodeStringValue(
                settings.settings().get("brokerUrl"), "tcp://localhost:61616"
        );
        String messageSelector = (String) settings.settings().get("messageSelector");
        String topicFilter = (String) settings.settings().get("topicFilter");

        apimConsumer = new APIMConsumer(
                new URI(brokerUrl),
                messageSelector,
                topicFilter,
                logger
        );
        apimConsumerThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(),
                "fedora-river-apimConsumer").newThread(apimConsumer);

        logger.info("created");
    }

    @Override
    public void start() {
        apimConsumerThread.start();
        logger.info("started");
    }

    @Override
    public void close() {
        apimConsumer.terminate();
        logger.info("closed");
    }
}
