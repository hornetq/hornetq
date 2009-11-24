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

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.tests.integration.cluster.failover.FailoverTestBase;

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

   private void doTestMixedPersistentAndNonPersistentMessagesOrderWithReplicatedBackup(boolean transactional) throws Exception
   {
      String address = randomString();
      String queue = randomString();

      ClientSessionFactory csf = new ClientSessionFactoryImpl(getConnectorTransportConfiguration(true));
      csf.setBlockOnNonPersistentSend(false);
      csf.setBlockOnPersistentSend(false);
      ClientSession session = null;
      if (transactional)
      {
         session = csf.createSession(false, false);
      }
      else
      {
         session = csf.createSession(true, true);
      }
      session.createQueue(address, queue, true);
      ClientProducer producer = session.createProducer(address);
      boolean durable = false;
      for (int i = 0; i < NUM; i++)
      {
         ClientMessage msg = session.createClientMessage(durable);
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

      csf = new ClientSessionFactoryImpl(getConnectorTransportConfiguration(true));
      session = csf.createSession(true, true);
      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < NUM; i++)
      {
         ClientMessage message = consumer.receive(1000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("counter").intValue());
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
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();
      if (!live)
      {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      return new TransportConfiguration(InVMAcceptorFactory.class.getName(), server1Params);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();
      if (!live)
      {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      return new TransportConfiguration(InVMConnectorFactory.class.getName(), server1Params);
   }

}
