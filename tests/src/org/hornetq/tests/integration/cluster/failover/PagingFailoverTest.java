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

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.SimpleString;

/**
 * A PagingFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PagingFailoverTest extends FailoverTestBase
{

   // Constants -----------------------------------------------------

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPage() throws Exception
   {
      internalTestPage(false, false);
   }

   public void testPageTransactioned() throws Exception
   {
      internalTestPage(true, false);
   }

   public void testPageTransactionedFailBeforeconsume() throws Exception
   {
      internalTestPage(true, true);
   }

   public void internalTestPage(final boolean transacted, final boolean failBeforeConsume) throws Exception
   {
      ClientSessionFactoryInternal factory = getSessionFactory();
      factory.setBlockOnPersistentSend(true);
      factory.setBlockOnAcknowledge(true);
      ClientSession session = factory.createSession(!transacted, !transacted, 0);

      try
      {

         session.createQueue(ADDRESS, ADDRESS, true);

         final CountDownLatch latch = new CountDownLatch(1);

         class MyListener implements SessionFailureListener
         {
            public void connectionFailed(HornetQException me)
            {
               latch.countDown();
            }

            public void beforeReconnect(HornetQException exception)
            {
            }

         }

         session.addFailureListener(new MyListener());

         ClientProducer prod = session.createProducer(ADDRESS);

         final int TOTAL_MESSAGES = 2000;

         for (int i = 0; i < TOTAL_MESSAGES; i++)
         {
            if (transacted && i % 10 == 0)
            {
               session.commit();
            }
            ClientMessage msg = session.createClientMessage(true);
            msg.putIntProperty(new SimpleString("key"), i);
            prod.send(msg);
         }

         session.commit();

         if (failBeforeConsume)
         {
            failSession(session, latch);
         }

         session.start();

         ClientConsumer cons = session.createConsumer(ADDRESS);

         final int MIDDLE = TOTAL_MESSAGES / 2;

         for (int i = 0; i < MIDDLE; i++)
         {
            ClientMessage msg = cons.receive(20000);
            assertNotNull(msg);
            msg.acknowledge();
            if (transacted && i % 10 == 0)
            {
               session.commit();
            }
            assertEquals((Integer)i, (Integer)msg.getObjectProperty(new SimpleString("key")));
         }

         session.commit();

         if (!failBeforeConsume)
         {
            failSession(session, latch);
         }

         session.close();

         session = factory.createSession(true, true, 0);

         cons = session.createConsumer(ADDRESS);

         session.start();

         for (int i = MIDDLE; i < TOTAL_MESSAGES; i++)
         {
            ClientMessage msg = cons.receive(5000);
            assertNotNull(msg);

            msg.acknowledge();
            int result = (Integer)msg.getObjectProperty(new SimpleString("key"));
            assertEquals(i, result);
         }
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   /**
    * @param session
    * @param latch
    * @throws InterruptedException
    */
   private void failSession(ClientSession session, final CountDownLatch latch) throws InterruptedException
   {
      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.failover.FailoverTestBase#getAcceptorTransportConfiguration(boolean)
    */
   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return getInVMTransportAcceptorConfiguration(live);
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.failover.FailoverTestBase#getConnectorTransportConfiguration(boolean)
    */
   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(boolean live)
   {
      return getInVMConnectorTransportConfiguration(live);
   }

   protected HornetQServer createServer(final boolean realFiles, final Configuration configuration)
   {
      return createServer(realFiles, configuration, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      server1Service = createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      server0Service = createServer(true, config0);

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
