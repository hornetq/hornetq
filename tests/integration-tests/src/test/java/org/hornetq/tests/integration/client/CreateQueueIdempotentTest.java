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

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.QueueExistsException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.ServiceTestBase;

public class CreateQueueIdempotentTest extends ServiceTestBase
{

   private HornetQServer server;

   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createDefaultConfig();
      conf.setSecurityEnabled(false);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));

      server = addServer(HornetQServers.newHornetQServer(conf, true));
      server.start();
   }

   public void testSequentialCreateQueueIdempotency() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("SequentialCreateQueueIdempotency");

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = addSessionFactory(locator.createSessionFactory());

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, true);

      try
      {
         session.createQueue(QUEUE, QUEUE, null, true);
         fail("Expected exception, queue already exists");
      }
      catch(QueueExistsException qee)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

    public void testConcurrentCreateQueueIdempotency() throws Exception
    {
        final String QUEUE = "ConcurrentCreateQueueIdempotency";
        AtomicInteger queuesCreated = new AtomicInteger(0);
        AtomicInteger failedAttempts = new AtomicInteger(0);

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
      server.start();

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
            locator = createInVMNonHALocator();
            ClientSessionFactory sf = locator.createSessionFactory();
                session = sf.createSession(false, true, true);
                final SimpleString QUEUE = new SimpleString(queueName);
                session.createQueue(QUEUE, QUEUE, null, true);
                queuesCreated.incrementAndGet();
            }
            catch(QueueExistsException qne)
            {
               failedAttempts.incrementAndGet();
            }
            catch (Exception e)
            {
                e.printStackTrace();
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
