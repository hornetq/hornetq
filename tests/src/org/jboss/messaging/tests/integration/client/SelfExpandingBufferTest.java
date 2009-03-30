/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

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

   MessagingServer service;

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

         MessagingBuffer buffer = msg.getBody();

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

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
