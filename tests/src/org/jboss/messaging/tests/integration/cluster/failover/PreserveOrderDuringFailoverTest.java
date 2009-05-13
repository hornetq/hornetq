/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A AutomaticFailoverWithDiscoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 * Created 8 Dec 2008 14:52:21
 *
 *
 */
public class PreserveOrderDuringFailoverTest extends FailoverTestBase
{
   private static final Logger log = Logger.getLogger(PreserveOrderDuringFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverOrderTestAddress");

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testOrdering() throws Exception
   {
      for (int i = 0; i < 20; i++)
      {
         log.info("testOrdering # " + i);
         setUpFailoverServers(false, -1, -1);
         failoverOrderTest();
         stopServers();
      }
   }

   protected void failoverOrderTest() throws Exception
   {
      ClientSessionFactory sf = createFailoverFactory();

      ClientSession session = sf.createSession(false, true, true);

      final RemotingConnection conn1 = ((ClientSessionImpl)session).getConnection();

      session.createQueue(ADDRESS, ADDRESS, null, false);

      Interceptor failInterceptor = new Interceptor()
      {
         int msg = 0;

         public boolean intercept(final Packet packet, final RemotingConnection conn) throws MessagingException
         {
            if (packet instanceof SessionSendMessage)
            {
               if (msg++ == 554)
               {
                  // Simulate failure on connection
                  conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));
                  return false;
               }
            }
            else
            {
               System.out.println("packet " + packet.getClass().getName());
            }

            return true;
         }
      };

      liveServer.getRemotingService().addInterceptor(failInterceptor);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      boolean outOfOrder = false;

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         if (i != (Integer)message2.getProperty(new SimpleString("count")))
         {
            System.out.println("Messages received out of order, " + i +
                               " != " +
                               message2.getProperty(new SimpleString("count")));
            outOfOrder = true;
         }

         message2.acknowledge();
      }

      session.close();

      session = sf.createSession(false, true, true);

      consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         if (i != (Integer)message2.getProperty(new SimpleString("count")))
         {
            System.out.println("Messages received out of order, " + i +
                               " != " +
                               message2.getProperty(new SimpleString("count")));
            outOfOrder = true;
         }

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      if (message3 != null)
      {
         do
         {
            System.out.println("Message " + message3.getProperty(new SimpleString("count")) + " was duplicated");
            message3 = consumer.receive(1000);
         }
         while (message3 != null);
         fail("Duplicated messages received on test");
      }

      session.close();

      assertFalse("Messages received out of order, look at System.out for more details", outOfOrder);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
