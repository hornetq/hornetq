/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.tests.integration.client;

import junit.framework.Assert;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.ServiceTestBase;

import java.util.concurrent.atomic.AtomicInteger;

public class CreateQueueIdempotentTest extends ServiceTestBase
{
    private static final Logger log = Logger.getLogger(CreateQueueIdempotentTest.class);

    // Constants -----------------------------------------------------

    // Attributes ----------------------------------------------------

    // Static --------------------------------------------------------

    // Constructors --------------------------------------------------

    // Public --------------------------------------------------------

    public void testSequentialCreateQueueIdempotency() throws Exception
    {
        boolean success = false;
        final SimpleString QUEUE = new SimpleString("SequentialCreateQueueIdempotency");

        Configuration conf = createDefaultConfig();

        conf.setSecurityEnabled(false);

        conf.getAcceptorConfigurations().add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));

        HornetQServer server = HornetQServers.newHornetQServer(conf, true);

        server.start();
        ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

        ClientSessionFactory sf = locator.createSessionFactory();

        ClientSession session = sf.createSession(false, true, true);

        session.createQueue(QUEUE, QUEUE, null, true);

        try
        {
            session.createQueue(QUEUE, QUEUE, null, true);
        }
        catch (Exception e)
        {
            if (e instanceof HornetQException)
            {
                if (((HornetQException) e).getCode() == 101)
                {
                    success = true;
                }
            }
        }

        session.close();

        locator.close();

        server.stop();
        
        Assert.assertTrue(success);
    }

    public void testConcurrentCreateQueueIdempotency() throws Exception
    {
        boolean success = true;
        final String QUEUE = "ConcurrentCreateQueueIdempotency";
        AtomicInteger queuesCreated = new AtomicInteger(0);
        AtomicInteger failedAttempts = new AtomicInteger(0);

        Configuration conf = createDefaultConfig();

        conf.setSecurityEnabled(false);

        conf.getAcceptorConfigurations().add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));

        HornetQServer server = HornetQServers.newHornetQServer(conf, true);

        server.start();
        final int NUM_THREADS = 5;
        
        QueueCreator[] queueCreators = new QueueCreator[NUM_THREADS];


        for(int i = 0; i < NUM_THREADS; i++)
        {
            QueueCreator queueCreator = new QueueCreator(QUEUE, queuesCreated, failedAttempts);
            queueCreators[i] = queueCreator;
        }

        for(int i = 0; i < NUM_THREADS; i++)
        {
            queueCreators[i].start();
        }

        for(int i = 0; i < NUM_THREADS; i++)
        {
            queueCreators[i].join();
        }

        server.stop();

        // re-starting the server appears to be an unreliable guide
        try
        {
            server.start();
        } catch (Exception e)
        {
            System.out.println("THIS BLEW UP!!");
            e.printStackTrace();
            success = false;
        }
        
        server.stop();

        Assert.assertTrue(success);
        Assert.assertEquals(1, queuesCreated.intValue());
        Assert.assertEquals(NUM_THREADS - 1, failedAttempts.intValue());
    }

    // Package protected ---------------------------------------------

    // Protected -----------------------------------------------------

    // Private -------------------------------------------------------

    // Inner classes -------------------------------------------------

    class QueueCreator extends Thread
    {
        private String queueName = null;
        private AtomicInteger queuesCreated = null;
        private AtomicInteger failedAttempts = null;


        QueueCreator(String queueName, AtomicInteger queuesCreated, AtomicInteger failedAttempts)
        {
            this.queueName = queueName;
            this.queuesCreated = queuesCreated;
            this.failedAttempts = failedAttempts;
        }
        @Override
        public void run()
        {
            ServerLocator locator = null;
            ClientSession session = null;

            try
            {
                locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
                ClientSessionFactory sf = locator.createSessionFactory();
                session = sf.createSession(false, true, true);
                final SimpleString QUEUE = new SimpleString(queueName);
                session.createQueue(QUEUE, QUEUE, null, true);
                queuesCreated.incrementAndGet();
            }
            catch (Exception e)
            {
                e.printStackTrace();
                if (e instanceof HornetQException)
                {
                    if (((HornetQException) e).getCode() == 101)
                    {
                        failedAttempts.incrementAndGet();
                    }
                }
            }
            finally
            {
                if (locator != null) {
                    locator.close();
                }
                if (session != null) {
                    try {
                        session.close();
                    } catch (HornetQException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
