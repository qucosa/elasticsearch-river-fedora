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

package de.slub.elasticsearch.river.fedora;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraCredentials;
import com.yourmediashelf.fedora.client.request.DescribeRepository;
import com.yourmediashelf.fedora.client.response.DescribeRepositoryResponse;
import com.yourmediashelf.fedora.generated.access.FedoraRepository;
import de.slub.fedora.jms.APIMConsumer;
import de.slub.index.DatastreamIndexJob;
import de.slub.index.IndexJob;
import de.slub.index.IndexJobProcessor;
import de.slub.index.ObjectIndexJob;
import de.slub.util.concurrent.UniqueDelayQueue;
import org.apache.activemq.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class FedoraRiver extends AbstractRiverComponent implements River {

    private static final String DEFAULT_INDEX_NAME = "fedora";
    private final Client esClient;
    private APIMConsumer apimConsumer;
    private IndexJobProcessor indexJobProcessor;
    private FedoraClient fedoraClient;
    private Thread apimConsumerThread;
    private Thread indexJobProcessorThread;
    private String brokerUrl;
    private String messageSelector;
    private String topicFilter;
    private String fedoraUrl;
    private String username;
    private String password;
    private String indexName;

    @Inject
    protected FedoraRiver(RiverName riverName, RiverSettings settings, Client client) throws Exception {
        super(riverName, settings);
        esClient = client;

        configure(settings);

        UniqueDelayQueue<IndexJob> indexJobQueue = new UniqueDelayQueue<>();

        setupApimConsumerThread(settings, indexJobQueue);
        setupFedoraClient();
        setupIndexJobProcessorThread(settings, client, indexJobQueue);

        logger.info("created");
    }

    @Override
    public void start() {
        setupIndex(esClient, indexName);
        apimConsumerThread.start();
        indexJobProcessorThread.start();
        logger.info("started");
    }

    @Override
    public void close() {
        apimConsumer.terminate();
        indexJobProcessor.terminate();
        logger.info("closed");
    }

    private void setupFedoraClient() throws Exception {
        try {
            fedoraClient = new FedoraClient(
                    new FedoraCredentials(fedoraUrl, username, password));
            DescribeRepositoryResponse describeResponse =
                    (DescribeRepositoryResponse) fedoraClient.execute(new DescribeRepository());

            FedoraRepository repoInfo = describeResponse.getRepositoryInfo();
            logger.info("Connected to [{}], version [{}] at [{}] as [{}]",
                    repoInfo.getRepositoryName(),
                    repoInfo.getRepositoryVersion(),
                    repoInfo.getRepositoryBaseURL(),
                    username);
        } catch (Exception ex) {
            logger.error("Error initializing Fedora connection: " + ex.getCause().getMessage());
            throw ex;
        }
    }

    private void configure(RiverSettings settings) throws ConfigurationException {
        indexName = XContentMapValues.nodeStringValue(
                settings.settings().get("indexName"), DEFAULT_INDEX_NAME
        );

        if (settings.settings().containsKey("jms")) {
            Map<String, Object> jmsSettings =
                    XContentMapValues.nodeMapValue(settings.settings().get("jms"), "jms");
            brokerUrl = (String) jmsSettings.get("brokerUrl");
            messageSelector = (String) jmsSettings.get("messageSelector");
            topicFilter = (String) jmsSettings.get("topicFilter");
        }

        if (brokerUrl == null || brokerUrl.isEmpty()) {
            throw new ConfigurationException("No broker URL has been configured. " +
                    "Please specify jms.brokerUrl in the Fedora River metadata."
            );
        }

        if (settings.settings().containsKey("fedora")) {
            Map<String, Object> fedoraSettings =
                    XContentMapValues.nodeMapValue(settings.settings().get("fedora"), "fedora");
            fedoraUrl = (String) fedoraSettings.get("url");
            username = (String) fedoraSettings.get("username");
            password = (String) fedoraSettings.get("password");
        } else {
            throw new ConfigurationException("No Fedora repository has been configured. " +
                    "Please specify fedora.* options in the Fedora River metadata.");
        }
    }

    private void setupIndexJobProcessorThread(RiverSettings settings, Client client, UniqueDelayQueue<IndexJob> indexJobQueue) {
        indexJobProcessor = new IndexJobProcessor(
                indexJobQueue,
                indexName,
                client,
                fedoraClient,
                logger
        );
        indexJobProcessorThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(),
                "fedora-river-indexJobProcessor").newThread(indexJobProcessor);
    }

    private void setupApimConsumerThread(RiverSettings settings, UniqueDelayQueue<IndexJob> indexJobQueue) throws URISyntaxException {
        apimConsumer = new APIMConsumer(
                new URI(brokerUrl),
                messageSelector,
                topicFilter,
                indexJobQueue,
                logger
        );
        apimConsumerThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(),
                "fedora-river-apimConsumer").newThread(apimConsumer);
    }

    private void setupIndex(Client esClient, String indexName) {
        IndicesExistsResponse response = esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
        if (response.isExists()) {
            logger.info("Found index {}", indexName);
        } else {
            esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
            logger.info("Created index {}", indexName);
        }

        prepareMapping(esClient, indexName, ObjectIndexJob.ES_TYPE_NAME,
                this.getClass().getResourceAsStream("/mapping-object.json"));
        prepareMapping(esClient, indexName, DatastreamIndexJob.ES_TYPE_NAME,
                this.getClass().getResourceAsStream("/mapping-datastream.json"));
    }

    private void prepareMapping(Client esClient, String indexName, String type, InputStream mapping) {
        try {
            esClient.admin().indices().preparePutMapping(indexName)
                    .setType(type)
                    .setSource(IOUtils.toString(mapping))
                    .execute()
                    .actionGet();
        } catch (IOException ex) {
            logger.error("Could not initialize mapping for {}/{}. Reason: {}",
                    indexName, type, ex.getMessage());
        }
    }

}
