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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexJobProcessor implements Runnable {

    private final Client client;
    private final BlockingQueue<IndexJob> queue;
    private final ESLogger log;
    private boolean terminated = false;

    public IndexJobProcessor(BlockingQueue<IndexJob> indexJobQueue, Client esClient, FedoraClient fedoraClient, ESLogger logger) {
        this.client = esClient;
        this.queue = indexJobQueue;
        this.log = logger;
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
            job.execute(client, log);
        } catch (Exception ex) {
            log.error("Error: " + ex.getMessage() + " Reason: " + ex.getCause().getMessage());
        }
    }
}
