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

import junit.framework.Assert;

import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A BlockingSendTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class BlockingSendTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSinglePersistentBlockingNonSync() throws Exception
   {
      HornetQServer server = createServer(true);
      ClientSession session = null;
      ClientSessionFactory factory = null;

      ServerLocator locator = null;
      try
      {

         server.getConfiguration().setJournalSyncNonTransactional(false);
         server.getConfiguration().setJournalBufferTimeout_AIO(15000);

         server.start();

         System.out.println("sync = " + server.getConfiguration().isJournalSyncNonTransactional());
         locator = createFactory(false);
         locator.setBlockOnDurableSend(true);
         factory = locator.createSessionFactory();

         session = factory.createSession();

         session.createQueue("address", "queue");

         ClientProducer prod = session.createProducer("address");

         ClientMessage message = session.createMessage(true);

         prod.send(message);

         ClientConsumer consumer = session.createConsumer("queue");

         session.start();

         ClientMessage msg = consumer.receive(5000);

         Assert.assertNotNull(msg);

         msg.acknowledge();

      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }
         if (factory != null)
         {
            factory.close();
         }

         if (session != null)
         {
            session.close();
         }

         if (server != null)
         {
            server.stop();
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
