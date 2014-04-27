package org.elasticsearch.river;

import org.elasticsearch.common.inject.Inject;

public class FedoraRiver extends AbstractRiverComponent implements River {

    @Inject
    protected FedoraRiver(RiverName riverName, RiverSettings settings) {
        super(riverName, settings);
        logger.info("create");
    }

    @Override
    public void start() {
        logger.info("start");
    }

    @Override
    public void close() {
        logger.info("close");
    }
}
