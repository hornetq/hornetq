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

package org.hornetq.byteman.tests;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A PagingTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *         <p/>
 *         Created Dec 5, 2008 8:25:58 PM
 */
@RunWith(BMUnitRunner.class)
public class PagingOMETest extends ServiceTestBase
{
   private ServerLocator locator;
   private HornetQServer server;
   private ClientSessionFactory sf;
   static final int MESSAGE_SIZE = 1024; // 1k

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final int RECEIVE_TIMEOUT = 5000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   static boolean failureActive = false;

   public static void refCheck()
   {
      if (failureActive)
      {
         throw new OutOfMemoryError("fake error");
      }
   }


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      failureActive = false;
      locator = createInVMNonHALocator();
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "fakeOME",
                     targetClass = "org.hornetq.core.paging.cursor.PagedReferenceImpl",
                     targetMethod = "getPagedMessage",
                     targetLocation = "ENTRY",
                     action = "org.hornetq.byteman.tests.PagingOMETest.refCheck()"
                  )
            }
      )
   public void testPageCleanup() throws Exception
   {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(false);

      server =
         createServer(true, config,
                      PagingOMETest.PAGE_SIZE,
                      PagingOMETest.PAGE_MAX,
                      new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 2;

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);
      locator.setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().startPaging();

      Assert.assertTrue(queue.getPageSubscription().getPagingStore().isPaging());

      ClientProducer producer = session.createProducer(PagingOMETest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++)
      {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++)
      {
         message = session.createMessage(true);

         HornetQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0)
         {
            session.commit();
         }
      }
      session.commit();

      session = sf.createSession(false, false, false);

      session.start();

      assertEquals(numberOfMessages, queue.getMessageCount());

      // The consumer has to be created after the queue.getMessageCount assertion
      // otherwise delivery could alter the messagecount and give us a false failure
      ClientConsumer consumer = session.createConsumer(PagingOMETest.ADDRESS);
      ClientMessage msg = null;

      msg = consumer.receive(1000);

      failureActive = true;
      msg.individualAcknowledge();
      try
      {
         session.commit();
         Assert.fail("exception expected");
      }
      catch (Exception expected)
      {
      }
      failureActive = false;
      session.rollback();

      session.close();

      sf.close();

      locator.close();

      server.stop();


      server.start();



      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);
      locator.setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      consumer = session.createConsumer(PagingOMETest.ADDRESS);

      session.start();

      for (int i = 0; i < numberOfMessages; i++)
      {
         msg = consumer.receive(1000);
         Assert.assertNotNull(msg);
         msg.individualAcknowledge();
      }
      Assert.assertNull(consumer.receiveImmediate());
      session.commit();

      session.close();
      sf.close();
      server.stop();

   }
}
