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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Ignore("No useful way to test XContentBuilder output")
public class TikaContentIndexerTest {

    @Test
    public void useTika() throws IOException {
        XContentBuilder contentBuilder = jsonBuilder();
        InputStream contentInputStream = this.getClass().getResourceAsStream("/documents/test.pdf");

        contentBuilder.startObject();

        TikaContentIndexer indexer = new TikaContentIndexer();
        indexer.index(contentBuilder, contentInputStream);

        contentBuilder.endObject();

        System.out.println(contentBuilder.string());
    }

}