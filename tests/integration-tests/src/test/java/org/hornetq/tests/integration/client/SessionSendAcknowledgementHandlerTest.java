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

package org.hornetq.tests.integration.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A SendAcknowledgementsTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 9 Feb 2009 13:29:19
 *
 *
 */
public class SessionSendAcknowledgementHandlerTest extends ServiceTestBase
{
   private HornetQServer server;

   private final SimpleString address = new SimpleString("address");

   private final SimpleString queueName = new SimpleString("queue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);
      server.start();
   }

   public void testSetInvalidSendACK() throws Exception
   {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(-1);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      try
      {

         boolean failed = false;
         try
         {
            session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
            {
               public void sendAcknowledged(Message message)
               {
               }
            });
         }
         catch (Throwable expected)
         {
            failed = true;
         }

         assertTrue("Expected a failure on setting ACK Handler", failed);

         session.createQueue(address, queueName, false);
      }
      finally
      {
         session.close();
      }
   }

   public void testSendAcknowledgementsNoWindowSize() throws Exception
   {
      testSendAcknowledgements(0);
   }

   public void testSendAcknowledgements() throws Exception
   {
      testSendAcknowledgements(1024);
   }

   public void testSendAcknowledgements(int windowSize) throws Exception
   {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(windowSize);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      session.createQueue(address, queueName, false);

      ClientProducer prod = session.createProducer(address);

      final int numMessages = 1000;

      final CountDownLatch latch = new CountDownLatch(numMessages);

      session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         public void sendAcknowledged(final Message message)
         {
            latch.countDown();
         }
      });

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = session.createMessage(false);

         prod.send(msg);
      }

      session.close();

      locator.close();
      boolean ok = latch.await(5000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }
}
