package de.slub.elasticsearch.river.fedora;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class FedoraRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(FedoraRiver.class).asEagerSingleton();
    }
}
