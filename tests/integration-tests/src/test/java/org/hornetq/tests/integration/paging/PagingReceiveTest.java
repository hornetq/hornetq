/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.paging;

import junit.framework.Assert;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class PagingReceiveTest extends ServiceTestBase {

    private static final SimpleString ADDRESS = new SimpleString("jms.queue.catalog-service.price.change.bm");

    private ServerLocator locator;

    protected boolean isNetty() {
        return false;
    }

    @Test
    public void testReceive1() throws Exception {
        testReceive("data.1.zip");
    }

    @Test
    public void testReceive2() throws Exception {
        testReceive("data.2.zip");
    }

    private void testReceive(final String dataPath) throws Exception {
        startServer(dataPath);
        ClientMessage message = receiveMessage();
        Assert.assertNotNull("Message not found.", message);
    }

    private void startServer(final String dataPath) throws Exception {
        final URL dataFile = this.getClass().getResource("/" + dataPath);
        final File file = new File(dataFile.toURI());
        unzip(file, new File(getTestDir()));

        final HornetQServer server = newHornetQServer();

        server.start();
        waitForServer(server);
        locator = createFactory(isNetty());
    }

    private ClientMessage receiveMessage() throws Exception {
        final ClientSessionFactory sf = createSessionFactory(locator);
        ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

        ClientConsumer consumer = session.createConsumer(ADDRESS);
        session.start();

        ClientMessage message = consumer.receive(1000);

        session.commit();

        if (message != null) {
            message.acknowledge();
        }

        consumer.close();

        session.close();

        return message;
    }

    private HornetQServer newHornetQServer() throws Exception {
        final HornetQServer server = createServer(true, isNetty());

        final AddressSettings settings = new AddressSettings();
        settings.setMaxSizeBytes(67108864);
        settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
        settings.setMaxRedeliveryDelay(3600000);
        settings.setRedeliveryMultiplier(2.0);
        settings.setRedeliveryDelay(500);

        server.getAddressSettingsRepository().addMatch("#", settings);

        return server;
    }

    public static void unzip(final File zipfile, final File directory) throws IOException {
        final ZipFile zfile = new ZipFile(zipfile);
        final Enumeration<? extends ZipEntry> entries = zfile.entries();
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            final File file = new File(directory, entry.getName());
            if (entry.isDirectory()) {
                file.mkdirs();
            } else {
                file.getParentFile().mkdirs();

                final InputStream in = zfile.getInputStream(entry);
                try {
                    copy(in, file);
                } finally {
                    in.close();
                }
            }
        }
    }

    private static void copy(final InputStream in, final OutputStream out) throws IOException {
        final byte[] buffer = new byte[1024];
        while (true) {
            final int readCount = in.read(buffer);
            if (readCount < 0) {
                break;
            }

            out.write(buffer, 0, readCount);
        }
    }

    private static void copy(final InputStream in, final File file) throws IOException {
        final OutputStream out = new FileOutputStream(file);
        try {
            copy(in, out);
        } finally {
            out.close();
        }
    }


}
