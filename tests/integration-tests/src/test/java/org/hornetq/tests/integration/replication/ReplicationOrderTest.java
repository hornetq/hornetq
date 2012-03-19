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

package org.hornetq.tests.integration.replication;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.tests.integration.cluster.failover.FailoverTestBase;
import org.hornetq.tests.util.RandomUtil;

/**
 * A ReplicationOrderTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ReplicationOrderTest extends FailoverTestBase
{

   public static final int NUM = 300;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup() throws Exception
   {
      doTestMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup(false);
   }

   public void testTxMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup() throws Exception
   {
      doTestMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup(true);
   }

   private void doTestMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup(final boolean transactional) throws Exception
   {
      String address = RandomUtil.randomString();
      String queue = RandomUtil.randomString();
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(getConnectorTransportConfiguration(true));
      addServerLocator(locator);
      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = null;
      if (transactional)
      {
         session = csf.createSession(false, false);
      }
      else
      {
         session = csf.createSession(true, true);
      }
      addClientSession(session);
      session.createQueue(address, queue, true);
      ClientProducer producer = session.createProducer(address);
      boolean durable = false;
      for (int i = 0; i < ReplicationOrderTest.NUM; i++)
      {
         ClientMessage msg = session.createMessage(durable);
         msg.putIntProperty("counter", i);
         producer.send(msg);
         if (transactional)
         {
            if (i % 10 == 0)
            {
               session.commit();
               durable = !durable;
            }
         }
         else
         {
            durable = !durable;
         }
      }
      if (transactional)
      {
         session.commit();
      }
      session.close();

      locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(getConnectorTransportConfiguration(true)));
      csf = createSessionFactory(locator);
      session = csf.createSession(true, true);
      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < ReplicationOrderTest.NUM; i++)
      {
         ClientMessage message = consumer.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("counter").intValue());
      }

      consumer.close();
      session.deleteQueue(queue);

      session.close();
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return createTransportConfiguration(INVM_CONNECTOR_FACTORY, live);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return createTransportConfiguration(INVM_ACCEPTOR_FACTORY, live);
   }

   private static TransportConfiguration createTransportConfiguration(String name, final boolean live)
   {
      Map<String, Object> serverParams = new HashMap<String, Object>();
      if (!live)
      {
         serverParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      return new TransportConfiguration(name, serverParams);
   }

}
