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

package de.slub.elasticsearch.river.fedora.de.slub.jms.fedora;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.elasticsearch.common.logging.ESLogger;

import javax.jms.*;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class APIMConsumer implements Runnable {

    private final URI uri;
    private final ESLogger log;
    private final String messageSelector;
    private final String topicFilter;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private boolean terminated = false;

    public APIMConsumer(URI broker, String messageSelector, String topicFilter, ESLogger logger) {
        this.uri = broker;
        this.log = logger;
        this.messageSelector = messageSelector;
        this.topicFilter = (topicFilter != null && !topicFilter.isEmpty()) ? topicFilter : "fedora.apim.*";
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

    public void terminate() {
        terminated = true;
    }

    private void receiveLoop() throws InterruptedException, JMSException {
        while (!terminated) {
            Message msg = consumer.receive(TimeUnit.SECONDS.toMillis(1));
            if ((msg != null) && (msg instanceof TextMessage)) {
                log.info("received: " + ((TextMessage) msg).getText());
            }
        }
    }

    private void startup() throws JMSException {
        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(uri);

        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic("fedora.apim.*");

        if (messageSelector == null || messageSelector.isEmpty()) {
            consumer = session.createConsumer(destination);
        } else {
            consumer = session.createConsumer(destination, messageSelector);
        }

        log.info("Connected to JMS broker: " + uri.toASCIIString());
    }

    private void shutdown() {
        try {
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
