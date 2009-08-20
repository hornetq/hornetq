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
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A SelfExpandingBufferTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 23, 2009 4:27:16 PM
 *
 *
 */
public class SelfExpandingBufferTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   HornetQServer service;

   SimpleString ADDRESS = new SimpleString("Address");

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSelfExpandingBufferNetty() throws Exception
   {
      testSelfExpandingBuffer(true);
   }

   public void testSelfExpandingBufferInVM() throws Exception
   {
      testSelfExpandingBuffer(false);
   }

   public void testSelfExpandingBuffer(boolean netty) throws Exception
   {

      setUpService(netty);

      ClientSessionFactory factory;

      if (netty)
      {
         factory = createNettyFactory();
      }
      else
      {
         factory = createInVMFactory();
      }

      ClientSession session = factory.createSession(false, true, true);

      try
      {

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientMessage msg = session.createClientMessage(true);

         HornetQBuffer buffer = msg.getBody();

         for (int i = 0; i < 10; i++)
         {
            buffer.writeBytes(new byte[1024]);
         }
         
         ClientProducer prod = session.createProducer(ADDRESS);
         
         prod.send(msg);
         
         ClientConsumer cons = session.createConsumer(ADDRESS);

         session.start();
         
         ClientMessage msg2 = cons.receive(3000);
         assertNotNull(msg2);
         
         
         assertEquals(1024 * 10, msg2.getBodySize());
         
         
      }
      finally
      {
         session.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUpService(boolean netty) throws Exception
   {
      service = createServer(false, createDefaultConfig(netty));
      service.start();
   }

   protected void tearDown() throws Exception
   {
      if (service != null && service.isStarted())
      {
         service.stop();
      }
      
      service = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
