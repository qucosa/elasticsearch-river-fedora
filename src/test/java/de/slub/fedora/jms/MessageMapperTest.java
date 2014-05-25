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

package de.slub.fedora.jms;

import de.slub.index.DatastreamIndexJob;
import de.slub.index.IndexJob;
import de.slub.index.ObjectIndexJob;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import javax.jms.TextMessage;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageMapperTest {

    @Test
    public void returnsNoIndexJobForGetObjectXmlMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test:4711");
        when(message.getStringProperty(eq("methodName"))).thenReturn("getObjectXML");
        when(message.getText()).thenReturn(getContent("/jms/getObjectXML.xml"));

        assertNull(MessageMapper.map(message));
    }

    @Test
    public void returnsIndexJobForIngestMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("ingest");
        when(message.getText()).thenReturn(getContent("/jms/ingest.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new ObjectIndexJob(IndexJob.Type.CREATE, "test-rest:1"),
                ij);
    }

    @Test
    public void returnsIndexJobForAddDatastreamMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("addDatastream");
        when(message.getText()).thenReturn(getContent("/jms/addDatastream.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new DatastreamIndexJob(IndexJob.Type.CREATE, "test-rest:1", "testAddDatastream"),
                ij);
    }

    @Test
    public void returnsIndexJobForModifyDatastreamByValueMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("modifyDatastreamByValue");
        when(message.getText()).thenReturn(getContent("/jms/modifyDatastreamByValue.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new DatastreamIndexJob(IndexJob.Type.UPDATE, "test-rest:1", "testModifyDatastream"),
                ij);
    }

    @Test
    public void returnsIndexJobForModifyObjectMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("modifyObject");
        when(message.getText()).thenReturn(getContent("/jms/modifyObject.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new ObjectIndexJob(IndexJob.Type.UPDATE, "test-rest:1"),
                ij);
    }

    @Test
    public void returnsIndexJobForpurgeDatastreamMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("purgeDatastream");
        when(message.getText()).thenReturn(getContent("/jms/purgeDatastream.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new DatastreamIndexJob(IndexJob.Type.DELETE, "test-rest:1", "testPurgeDatastream"),
                ij);
    }

    @Test
    public void returnsIndexJobForPurgeObjectMessage() throws Exception {
        TextMessage message = mock(TextMessage.class);
        when(message.getStringProperty(eq("pid"))).thenReturn("test-rest:1");
        when(message.getStringProperty(eq("methodName"))).thenReturn("purgeObject");
        when(message.getText()).thenReturn(getContent("/jms/purgeObject.xml"));

        IndexJob ij = MessageMapper.map(message);

        assertEquals(
                new ObjectIndexJob(IndexJob.Type.DELETE, "test-rest:1"),
                ij);
    }

    private String getContent(String filename) throws IOException {
        return IOUtils.toString(this.getClass().getResourceAsStream(filename));
    }


}
