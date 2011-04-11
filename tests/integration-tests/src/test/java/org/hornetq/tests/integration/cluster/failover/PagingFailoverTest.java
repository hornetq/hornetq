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

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * A PagingFailoverTest
 * 
 * TODO: validate replication failover also
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
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();
      super.tearDown();
   }

   public void testPageFailBeforeConsume() throws Exception
   {
      internalTestPage(false, true);
   }


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
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);

      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      ClientSession session = sf.createSession(!transacted, !transacted, 0);

      try
      {

         session.createQueue(PagingFailoverTest.ADDRESS, PagingFailoverTest.ADDRESS, true);

         ClientProducer prod = session.createProducer(PagingFailoverTest.ADDRESS);

         final int TOTAL_MESSAGES = 2000;

         for (int i = 0; i < TOTAL_MESSAGES; i++)
         {
            if (transacted && i % 10 == 0)
            {
               session.commit();
            }
            ClientMessage msg = session.createMessage(true);
            msg.putIntProperty(new SimpleString("key"), i);
            prod.send(msg);
         }

         session.commit();

         if (failBeforeConsume)
         {
            crash(session);
         }
         
         
         session.close();
         
         session = sf.createSession(!transacted, !transacted, 0);

         session.start();

         ClientConsumer cons = session.createConsumer(PagingFailoverTest.ADDRESS);

         final int MIDDLE = TOTAL_MESSAGES / 2;

         for (int i = 0; i < MIDDLE; i++)
         {
            System.out.println("msg " + i);
            ClientMessage msg = cons.receive(20000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            if (transacted && i % 10 == 0)
            {
               session.commit();
            }
            Assert.assertEquals(i, msg.getObjectProperty(new SimpleString("key")));
         }

         session.commit();
         
         cons.close();
         
         Thread.sleep(1000);

         if (!failBeforeConsume)
         {
            crash(session);
            // failSession(session, latch);
         }

         session.close();
         
         session = sf.createSession(true, true, 0);

         cons = session.createConsumer(PagingFailoverTest.ADDRESS);

         session.start();

         for (int i = MIDDLE; i < TOTAL_MESSAGES; i++)
         {
            ClientMessage msg = cons.receive(5000);
            Assert.assertNotNull(msg);

            msg.acknowledge();
            int result = (Integer)msg.getObjectProperty(new SimpleString("key"));
            Assert.assertEquals(i, result);
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
   private void failSession(final ClientSession session, final CountDownLatch latch) throws InterruptedException
   {
      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      Assert.assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return getInVMTransportAcceptorConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getInVMConnectorTransportConfiguration(live);
   }

   @Override
   protected HornetQServer createServer(final boolean realFiles, final Configuration configuration)
   {
      return createInVMFailoverServer(true,
                                      configuration,
                                      PagingFailoverTest.PAGE_SIZE,
                                      PagingFailoverTest.PAGE_MAX,
                                      new HashMap<String, AddressSettings>(),
                                      nodeManager);
   }

   @Override
   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(createServer(true, backupConfig));
   }

   @Override
   protected TestableServer createLiveServer()
   {
      return new SameProcessHornetQServer(createServer(true, liveConfig));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
