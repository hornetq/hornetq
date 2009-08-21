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

package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.utils.SimpleString;

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
public class AutomaticFailoverWithDiscoveryTest extends FailoverTestBase
{
   private static final Logger log = Logger.getLogger(AutomaticFailoverWithDiscoveryTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private final String groupAddress = "230.1.2.3";

   private final int groupPort = 8765;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailover() throws Exception
   {
      ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(groupAddress, groupPort);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      RemotingConnection conn1 = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));

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

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      setupGroupServers(false, "bc1", 5432, groupAddress, groupPort);
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
