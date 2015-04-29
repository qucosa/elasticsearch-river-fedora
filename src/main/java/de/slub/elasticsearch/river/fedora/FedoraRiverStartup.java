/*
 * Copyright 2015 SLUB Dresden
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

import de.slub.index.DatastreamIndexJob;
import de.slub.index.IndexJobProcessor;
import de.slub.index.ObjectIndexJob;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

class FedoraRiverStartup implements Runnable {

    private Map<String, Object> disseminationContentMapping;
    private Client esClient;
    private String indexName;
    private ESLogger logger;
    private List<Thread> threads;

    @Override
    public void run() {
        logger.info("Setup index and mapping");
        setupIndex(esClient, indexName);
        logger.info("Starting threads...");
        for (Thread t : threads) safeStart(t);
        logger.info("River startup complete");
    }

    public FedoraRiverStartup esClient(Client esClient) {
        this.esClient = esClient;
        return this;
    }

    public FedoraRiverStartup indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public FedoraRiverStartup threads(List<Thread> threads) {
        this.threads = threads;
        return this;
    }

    private void safeStart(Thread thread) {
        if (thread != null && thread.getState().equals(Thread.State.NEW)) {
            thread.start();
            logger.info("Started thread: [{}] {}", thread.getId(), thread.getName());
        }
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

        if (disseminationContentMapping != null && !disseminationContentMapping.isEmpty()) {
            prepareMapping(esClient, indexName, ObjectIndexJob.ES_TYPE_NAME,
                    disseminationContentMapping);
        }

        prepareMapping(esClient, indexName, DatastreamIndexJob.ES_TYPE_NAME,
                this.getClass().getResourceAsStream("/mapping-datastream.json"));
        prepareMapping(esClient, indexName, IndexJobProcessor.ES_ERROR_TYPE_NAME,
                this.getClass().getResourceAsStream("/mapping-error.json"));

    }

    private void prepareMapping(Client esClient, String indexName, String esTypeName, Map<String, Object> properties) {
        try {
            esClient.admin().indices().preparePutMapping(indexName)
                    .setType(esTypeName)
                    .setSource(properties)
                    .execute()
                    .actionGet();
        } catch (Exception ex) {
            logger.error("Could not initialize mapping for {}/{}. Reason: {}",
                    indexName, esTypeName, ex.getMessage());
        }
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

    public FedoraRiverStartup logger(ESLogger logger) {
        this.logger = logger;
        return this;
    }

    public FedoraRiverStartup disseminationContentMapping(Map<String, Object> disseminationContentMapping) {
        this.disseminationContentMapping = disseminationContentMapping;
        return this;
    }
}
