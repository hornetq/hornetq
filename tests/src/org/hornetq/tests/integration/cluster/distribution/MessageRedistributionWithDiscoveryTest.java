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

package org.hornetq.tests.integration.cluster.distribution;

import java.util.ArrayList;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * A MessageRedistributionWithDiscoveryTest
 *
 * @author clebertsuconic
 *
 *
 */
public class MessageRedistributionWithDiscoveryTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(SymmetricClusterWithDiscoveryTest.class);

   protected final String groupAddress = getUDPDiscoveryAddress();

   protected final int groupPort = getUDPDiscoveryPort();

   protected boolean isNetty()
   {
      return false;
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      setupCluster();
   }

   protected void tearDown() throws Exception
   {
      for (int i = 0; i < servers.length; i++)
      {
         if (servers[i] != null)
         {
            servers[i].stop();
            servers[i] = null;
         }
      }
      super.tearDown();
   }

   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      for (int i = 0; i < 5; i++)
      {
         setServer(forwardWhenNoConsumers, i);
      }
   }

   /**
    * @param forwardWhenNoConsumers
    */
   protected void setServer(final boolean forwardWhenNoConsumers, int server)
   {
      setupLiveServerWithDiscovery(server,
                                   groupAddress,
                                   groupPort,
                                   isFileStorage(),
                                   isNetty(),
                                   false);

      AddressSettings setting = new AddressSettings();
      setting.setRedeliveryDelay(0);
      setting.setRedistributionDelay(0);

      servers[server].getAddressSettingsRepository().addMatch("#", setting);

      setupDiscoveryClusterConnection("cluster" + server, server, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   public void testRedistributeWithPreparedAndRestart() throws Exception
   {
      startServers(0);

      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);

      ClientSession session0 = sfs[0].createSession(false, false, false);

      ClientProducer prod0 = session0.createProducer("queues.testaddress");

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = session0.createMessage(true);

         msg.putIntProperty("key", i);

         prod0.send(msg);

         session0.commit();
      }

      session0.close();

      session0 = sfs[0].createSession(true, false, false);

      ClientConsumer consumer0 = session0.createConsumer("queue0");

      session0.start();

      ArrayList<Xid> xids = new ArrayList<Xid>();

      for (int i = 0; i < 100; i++)
      {
         Xid xid = newXID();

         session0.start(xid, XAResource.TMNOFLAGS);

         ClientMessage msg = consumer0.receive(5000);

         msg.acknowledge();

         session0.end(xid, XAResource.TMSUCCESS);

         session0.prepare(xid);

         xids.add(xid);
      }

      session0.close();

      sfs[0].close();
      sfs[0] = null;

      servers[0].stop();
      servers[0] = null;

      setServer(false, 0);

      startServers(1, 2);

      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      ClientSession session1 = sfs[1].createSession(false, false);

      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      session1.start();
      ClientConsumer consumer1 = session1.createConsumer("queue0");

      startServers(0);

      setupSessionFactory(0, isNetty());

      waitForBindings(0, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);

      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      session0 = sfs[0].createSession(true, false, false);

      for (Xid xid : xids)
      {
         session0.rollback(xid);
      }

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = consumer1.receive(15000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session1.commit();

   }

}
