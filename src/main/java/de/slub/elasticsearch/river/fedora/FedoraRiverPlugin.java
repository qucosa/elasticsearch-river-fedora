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
