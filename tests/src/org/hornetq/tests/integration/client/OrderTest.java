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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A OrderTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class OrderTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleOrderNoStorage() throws Exception
   {
      doTestSimpleOrder(false);
   }

   public void testSimpleOrderPersistence() throws Exception
   {
      doTestSimpleOrder(true);
   }

   public void doTestSimpleOrder(final boolean persistent) throws Exception
   {
      server = createServer(persistent, true);
      server.start();

      ClientSessionFactory sf = createNettyFactory();

      sf.setBlockOnNonPersistentSend(false);
      sf.setBlockOnPersistentSend(false);
      sf.setBlockOnAcknowledge(true);

      ClientSession session = sf.createSession(true, true, 0);

      try
      {
         session.createQueue("queue", "queue", true);

         ClientProducer prod = session.createProducer("queue");

         for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = session.createClientMessage(i % 2 == 0);
            msg.setBody(session.createBuffer(new byte[1024]));
            msg.putIntProperty("id", i);
            prod.send(msg);
         }

         session.close();

         boolean started = false;

         for (int start = 0; start < 2; start++)
         {

            if (persistent && start == 1)
            {
               started = true;
               server.stop();
               server.start();
            }
            
            session = sf.createSession(true, true);

            session.start();

            ClientConsumer cons = session.createConsumer("queue");

            for (int i = 0; i < 100; i++)
            {
               if (!started || started && i % 2 == 0)
               {
                  ClientMessage msg = cons.receive(10000);
                  assertEquals(i, msg.getIntProperty("id").intValue());
               }
            }

            cons.close();

            cons = session.createConsumer("queue");

            for (int i = 0; i < 100; i++)
            {
               if (!started || started && i % 2 == 0)
               {
                  ClientMessage msg = cons.receive(10000);
                  assertEquals(i, msg.getIntProperty("id").intValue());
               }
            }

            session.close();
         }

      }
      finally
      {
         sf.close();
         session.close();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
