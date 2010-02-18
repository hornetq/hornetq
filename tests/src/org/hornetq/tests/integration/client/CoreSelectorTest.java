/*
 * Copyright 2010 Red Hat, Inc.
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

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A CoreSelectorTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class CoreSelectorTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testSelector() throws Exception
   {
      HornetQServer server = createServer(false, false);
      
      server.start();
      
      ClientSessionFactory factory = createInVMFactory();
      ClientSession session = factory.createSession();
      
      try
      {
         session.createQueue("queue", "queue");
         ClientProducer prod = session.createProducer("queue");
         
         
         ClientMessage msg = session.createMessage(false);
         msg.putIntProperty("intValue", 1);
         
         msg.putIntProperty("intValue", 1);
         msg.putBytesProperty("bValue", new byte[]{'1'});
         
         prod.send(msg);

         msg = session.createMessage(false);
         msg.putIntProperty("intValue", 2);
         
         session.start();
         
         ClientConsumer cons = session.createConsumer("queue", "bValue=1");
         
         assertNull(cons.receiveImmediate());
         
         cons.close();
         
         cons = session.createConsumer("queue", "intValue=1");
         
         msg = cons.receive(5000);
         
         assertNotNull(msg);
         
         assertEquals(1, (int)msg.getIntProperty("intValue"));
         
         assertNull(cons.receiveImmediate());
         
         
         
         
         
         
      }
      finally
      {
         session.close();
         server.stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
