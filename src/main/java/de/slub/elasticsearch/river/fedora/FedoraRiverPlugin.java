package de.slub.elasticsearch.river.fedora;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class FedoraRiverPlugin extends AbstractPlugin {
    @Inject
    public FedoraRiverPlugin(Settings settings) {
        super();
    }

    @Override
    public String name() {
        return "fedora-river";
    }

    @Override
    public String description() {
        return "Fedora river plugin";
    }

    @SuppressWarnings("UnusedDeclaration")
    public void onModule(RiversModule module) {
        module.registerRiver("fedora-river", FedoraRiverModule.class);
    }
}
