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

package org.hornetq.tests.stress.client;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A SendStressTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SendStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Remove this method to re-enable those tests
   @Test
   public void testStressSendNetty() throws Exception
   {
      doTestStressSend(true);
   }

   @Test
   public void testStressSendInVM() throws Exception
   {
      doTestStressSend(false);
   }

   public void doTestStressSend(final boolean netty) throws Exception
   {
      HornetQServer server = createServer(false, netty);
      server.start();
      ServerLocator locator = createNonHALocator(netty);
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = null;

      final int batchSize = 2000;

      final int numberOfMessages = 100000;

      try
      {
         server.start();

         session = sf.createSession(false, false);

         session.createQueue("address", "queue");

         ClientProducer producer = session.createProducer("address");

         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(new byte[1024]);

         for (int i = 0; i < numberOfMessages; i++)
         {
            producer.send(message);
            if (i % batchSize == 0)
            {
               System.out.println("Sent " + i);
               session.commit();
            }
         }

         session.commit();

         session.close();

         session = sf.createSession(false, false);

         ClientConsumer consumer = session.createConsumer("queue");

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();

            if (i % batchSize == 0)
            {
               System.out.println("Consumed " + i);
               session.commit();
            }
         }

         session.commit();
      }
      finally
      {
         if (session != null)
         {
            try
            {
               sf.close();
               session.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
         locator.close();
         server.stop();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
