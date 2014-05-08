package de.slub.elasticsearch.river.fedora;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

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
