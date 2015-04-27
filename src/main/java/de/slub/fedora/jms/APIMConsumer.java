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

import de.slub.index.IndexJob;
import de.slub.util.TerminateableRunnable;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.elasticsearch.common.logging.ESLogger;

import javax.jms.*;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static de.slub.fedora.jms.MessageMapper.map;

public class APIMConsumer extends TerminateableRunnable {

    private final URI uri;
    private final ESLogger log;
    private final String messageSelector;
    private final String topicFilter;
    private final java.util.Queue<IndexJob> indexJobQueue;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public APIMConsumer(URI broker, String messageSelector, String topicFilter, java.util.Queue<IndexJob> indexJobQueue, ESLogger logger) {
        this.uri = broker;
        this.log = logger;
        this.messageSelector = messageSelector;
        this.topicFilter = (topicFilter != null && !topicFilter.isEmpty()) ? topicFilter : "fedora.apim.*";
        this.indexJobQueue = indexJobQueue;
    }

    @Override
    public void run() {
        try {
            startup();
            receiveLoop();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            shutdown();
        }
    }

    private void receiveLoop() {
        while (isRunning()) {
            Message msg = null;
            try {
                msg = consumer.receive(TimeUnit.SECONDS.toMillis(1));
            } catch (JMSException e) {
                log.error("Failed receiving message: " + e.getMessage());
            }
            if ((msg != null) && (msg instanceof TextMessage)) {
                try {
                    log.debug("received:\n" + ((TextMessage) msg).getText());
                    scheduleJobs(map(msg));
                } catch (Exception e) {
                    log.error("Failed creating index job: " + e.getMessage());
                }
            }
        }
    }

    private void scheduleJobs(List<IndexJob> indexJobs) {
        if (indexJobs.isEmpty()) {
            log.debug("No index jobs scheduled: No mapping for JMS message.");
        } else {
            if (indexJobQueue.addAll(indexJobs)) {
                log.debug("Index jobs scheduled: %d", indexJobs.size());
            } else {
                log.debug("No index jobs scheduled: PID/DSID doesn't match or jobs are scheduled already.");
            }
        }
    }

    private void startup() throws JMSException {
        log.info("Set up ActiveMQ connection to {}...", uri.toASCIIString());

        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(uri);
        connection = connectionFactory.createConnection();
        connection.start();

        log.debug("Connection started!");

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(topicFilter);

        if (messageSelector == null || messageSelector.isEmpty()) {
            log.debug("Create message consumer");
            consumer = session.createConsumer(destination);
        } else {
            log.debug("Create message consumer with selector: {}", messageSelector);
            consumer = session.createConsumer(destination, messageSelector);
        }

        log.info("Connected to JMS broker: " + uri.toASCIIString());
    }

    private void shutdown() {
        try {
            log.debug("Shutdown...");
            consumer.close();
        } catch (JMSException e) {
            log.error("Error closing ActiveMQ consumer: " + e.getMessage());
        }
        try {
            session.close();
        } catch (JMSException e) {
            log.error("Error closing ActiveMQ session: " + e.getMessage());
        }
        try {
            connection.close();
        } catch (JMSException e) {
            log.error("Error closing ActiveMQ connection: " + e.getMessage());
        }
        log.info("Disconnected from JMS broker: " + uri.toASCIIString());
    }
}
