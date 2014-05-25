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

package de.slub.util.xslt;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Ignore;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class JsonTransformerTest {

    @Test
    @Ignore("Manual checking JSON transformation output")
    public void transformToJson() throws Exception {
        JsonTransformer transformer = JsonTransformer.getInstance();

        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                transformer.toJson(this.getClass().getResourceAsStream("/response/datastreamContent.xml"))
        );
        parser.nextToken();

        XContentBuilder jb = jsonBuilder().copyCurrentStructure(parser);

        System.out.println(jb.string());
    }

}