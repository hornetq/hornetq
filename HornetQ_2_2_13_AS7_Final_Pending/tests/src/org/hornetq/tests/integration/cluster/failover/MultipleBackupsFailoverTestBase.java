/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A MultipleBackupsFailoverTestBase
 *
 * @author jmesnil
 *
 *
 */
public abstract class MultipleBackupsFailoverTestBase extends ServiceTestBase
{
   Logger log = Logger.getLogger(this.getClass());
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static -------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract boolean isNetty();

   protected int waitForNewLive(long seconds,
                                boolean waitForNewBackup,
                                Map<Integer, TestableServer> servers,
                                int... nodes)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      int newLive = -1;
      while (true)
      {
         for (int node : nodes)
         {
            TestableServer backupServer = servers.get(node);
            if (newLive == -1 && backupServer.isInitialised())
            {
               newLive = node;
            }
            else if (newLive != -1)
            {
               if (waitForNewBackup)
               {
                  if (node != newLive && servers.get(node).isStarted())
                  {
                     return newLive;
                  }
               }
               else
               {
                  return newLive;
               }
            }
         }

         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            // ignore
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started");
         }
      }
   }

   protected ClientSession sendAndConsume(final ClientSessionFactory sf, final boolean createQueue) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      if (createQueue)
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, false);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(10000);

         assertNotNull(message2);

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      Assert.assertNull(message3);

      return session;
   }

   protected ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator,
                                                                                 int topologyMembers) throws Exception
   {
      return createSessionFactoryAndWaitForTopology(locator, topologyMembers, null);
   }

   protected ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator,
                                                                                 int topologyMembers,
                                                                                 HornetQServer server) throws Exception
   {
      ClientSessionFactoryInternal sf;
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

      LatchClusterTopologyListener topListener = new LatchClusterTopologyListener(countDownLatch);
      locator.addClusterTopologyListener(topListener);

      sf = (ClientSessionFactoryInternal)locator.createSessionFactory();

      boolean ok = countDownLatch.await(5, TimeUnit.SECONDS);
      locator.removeClusterTopologyListener(topListener);
      if (!ok)
      {
         if (server != null)
         {
            log.info("failed topology, Topology on server = " + server.getClusterManager().describe());
         }
      }
      assertTrue("expected " + topologyMembers + " members", ok);
      return sf;
   }

   public ServerLocator getServerLocator(int... nodes)
   {
      TransportConfiguration[] configs = new TransportConfiguration[nodes.length];
      for (int i = 0, configsLength = configs.length; i < configsLength; i++)
      {
         configs[i] = createTransportConfiguration(isNetty(), false, generateParams(nodes[i], isNetty()));
      }
      return new ServerLocatorImpl(true, configs);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class LatchClusterTopologyListener implements ClusterTopologyListener
   {
      final CountDownLatch latch;

      int liveNodes = 0;

      int backUpNodes = 0;

      List<String> liveNode = new ArrayList<String>();

      List<String> backupNode = new ArrayList<String>();

      public LatchClusterTopologyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void nodeUP(final long uniqueEventID,
                         String nodeID,
                         Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                         boolean last)
      {
         if (connectorPair.getA() != null && !liveNode.contains(connectorPair.getA().getName()))
         {
            liveNode.add(connectorPair.getA().getName());
            latch.countDown();
         }
         if (connectorPair.getB() != null && !backupNode.contains(connectorPair.getB().getName()))
         {
            backupNode.add(connectorPair.getB().getName());
            latch.countDown();
         }
      }

      public void nodeDown(final long uniqueEventID, String nodeID)
      {
      }
   }
}
