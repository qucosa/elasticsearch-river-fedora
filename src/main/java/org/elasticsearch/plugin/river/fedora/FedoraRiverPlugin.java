package org.elasticsearch.plugin.river.fedora;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.FedoraRiverModule;
import org.elasticsearch.river.RiversModule;

public class FedoraRiverPlugin extends AbstractPlugin {
    @Inject
    public FedoraRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-fedora";
    }

    @Override
    public String description() {
        return "River Fedora Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("fedora", FedoraRiverModule.class);
    }
}
