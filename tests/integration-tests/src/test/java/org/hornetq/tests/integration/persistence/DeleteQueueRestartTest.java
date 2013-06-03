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

package org.hornetq.tests.integration.persistence;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A DeleteMessagesRestartTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Mar 2, 2009 10:14:38 AM
 *
 *
 */
public class DeleteQueueRestartTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final String ADDRESS = "ADDRESS";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testDeleteQueueAndRestart() throws Exception
   {
      // This test could eventually pass, even when the queue was being deleted in the wrong order,
      // however it failed in 90% of the runs with 5 iterations.
      for (int i = 0; i < 5; i++)
      {
         setUp();
         internalDeleteQueueAndRestart();
         tearDown();
      }
   }

   private void internalDeleteQueueAndRestart() throws Exception
   {
      HornetQServer server = createServer(true);

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      locator.setBlockOnDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      locator.setMinLargeMessageSize(1024 * 1024);

      ClientSessionFactory factory = createSessionFactory(locator);

      final ClientSession session = factory.createSession(false, true, true);

      session.createQueue(DeleteQueueRestartTest.ADDRESS, DeleteQueueRestartTest.ADDRESS, true);

      ClientProducer prod = session.createProducer(DeleteQueueRestartTest.ADDRESS);

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = createBytesMessage(session, HornetQBytesMessage.TYPE, new byte[0], true);
         prod.send(msg);
      }

      final CountDownLatch count = new CountDownLatch(1);

      // Using another thread, as the deleteQueue is a blocked call
      new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               session.deleteQueue(DeleteQueueRestartTest.ADDRESS);
               session.close();
               count.countDown();
            }
            catch (HornetQException e)
            {
            }
         }
      }.start();

      Assert.assertTrue(count.await(5, TimeUnit.SECONDS));

      server.stop();

      server.start();

      server.stop();

   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
