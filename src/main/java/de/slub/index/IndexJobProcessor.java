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

package de.slub.index;

import com.yourmediashelf.fedora.client.FedoraClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexJobProcessor implements Runnable {

    public static final String ES_ERROR_TYPE_NAME = "error";
    private final Client client;
    private final BlockingQueue<IndexJob> queue;
    private final ESLogger log;
    private final FedoraClient fedoraClient;
    private final String indexName;
    private final String sdefPid;
    private final String method;
    private boolean terminated = false;

    public IndexJobProcessor(BlockingQueue<IndexJob> indexJobQueue, String indexName, Client esClient,
                             FedoraClient fedoraClient, ESLogger logger, String sdefPid, String method) {
        this.client = esClient;
        this.queue = indexJobQueue;
        this.indexName = indexName;
        this.fedoraClient = fedoraClient;
        this.log = logger;
        this.sdefPid = sdefPid;
        this.method = method;
    }

    public IndexJobProcessor(BlockingQueue<IndexJob> indexJobQueue, String indexName, Client esClient,
                             FedoraClient fedoraClient, ESLogger logger) {
        this(indexJobQueue, indexName, esClient, fedoraClient, logger, "", "");
    }

    public void terminate() {
        terminated = true;
    }

    @Override
    public void run() {
        try {
            while (!terminated) {
                IndexJob job = queue.poll(1, TimeUnit.SECONDS);
                if (job != null) {
                    perform(job);
                }
            }
        } catch (Exception ex) {
            log.error("Error: " + ex.getMessage() + " Reason: " + ex.getCause().getMessage());
        }
    }

    private void perform(IndexJob job) {
        log.debug("Performing: " + job);
        try {
            List<IndexJob> newJobs = job
                    .index(indexName)
                    .sdefPid(sdefPid)
                    .method(method)
                    .execute(fedoraClient, client, log);

            if (newJobs != null) for (IndexJob indexJob : newJobs) queue.add(indexJob);

        } catch (Exception ex) {
            log.error("Error: " + ex.getMessage());

            try {
                client.prepareIndex(indexName, ES_ERROR_TYPE_NAME, job.esid())
                        .setSource(
                                jsonBuilder().startObject()
                                        .field("PID", job.pid())
                                        .field("DSID", job.dsid())
                                        .field("job", job.toString())
                                        .field("message", ex.getMessage())
                                        .field("timestamp", new Date())
                                        .endObject()
                        ).execute().actionGet();
            } catch (Exception e) {
                log.error("Cannot write index error to node: " + e.getMessage());
            }

        }
    }
}
