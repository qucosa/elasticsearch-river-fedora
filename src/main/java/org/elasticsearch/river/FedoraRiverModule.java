package org.elasticsearch.river;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class FedoraRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(FedoraRiver.class).asEagerSingleton();
    }
}
