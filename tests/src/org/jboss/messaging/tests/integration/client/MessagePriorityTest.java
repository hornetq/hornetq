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

import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A MessagePriorityTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class MessagePriorityTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMessagePriority() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);

      ClientMessage[] messages = new ClientMessage[10];
      for (int i = 0; i < 10; i++)
      {
         ClientMessage m = createTextMessage(Integer.toString(i), session);
         m.setPriority((byte)i);
         messages[i] = m;
      }
      // send message with lower priority first
      for (int i = 0; i < 10; i++)
      {
         producer.send(messages[i]);
      }

      ClientConsumer consumer = session.createConsumer(queue);

      session.start();

      // expect to consumer message with higher priority first
      for (int i = 9; i >= 0; i--)
      {
         ClientMessage m = consumer.receive(500);
         assertNotNull(m);
         assertEquals(messages[i].getPriority(), m.getPriority());
         assertEquals(m.getBody().readString(), messages[i].getBody().readString());
      }

      consumer.close();
      session.deleteQueue(queue);
   }
   
   /**
    * in this tests, the session is started and the consumer created *before* the messages are sent.
    * each message which is sent will be received by the consumer in its buffer and the priority won't be taken
    * into account.
    * We need to implement client-side message priority to handle this case: https://jira.jboss.org/jira/browse/JBMESSAGING-1560
    */
   public void testMessagePriorityWithClientSidePrioritization() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      session.start();
      ClientConsumer consumer = session.createConsumer(queue);


      ClientMessage[] messages = new ClientMessage[10];
      for (int i = 0; i < 10; i++)
      {
         ClientMessage m = createTextMessage(Integer.toString(i), session);
         m.setPriority((byte)i);
         messages[i] = m;
      }
      // send message with lower priority first
      for (int i = 0; i < 10; i++)
      {
         producer.send(messages[i]);
      }

      // expect to consumer message with higher priority first
      for (int i = 9; i >= 0; i--)
      {
         ClientMessage m = consumer.receive(500);
         assertNotNull(m);
         assertEquals(messages[i].getPriority(), m.getPriority());
         assertEquals(m.getBody().readString(), messages[i].getBody().readString());
      }

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testMessageOrderWithSamePriority() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);

      ClientMessage[] messages = new ClientMessage[10];

      // send 3 messages with priority 0
      //      3                        7
      //      3                        3
      //      1                        9
      messages[0] = createTextMessage("a", session);
      messages[0].setPriority((byte)0);
      messages[1] = createTextMessage("b", session);
      messages[1].setPriority((byte)0);
      messages[2] = createTextMessage("c", session);
      messages[2].setPriority((byte)0);

      messages[3] = createTextMessage("d", session);
      messages[3].setPriority((byte)7);
      messages[4] = createTextMessage("e", session);
      messages[4].setPriority((byte)7);
      messages[5] = createTextMessage("f", session);
      messages[5].setPriority((byte)7);

      messages[6] = createTextMessage("g", session);
      messages[6].setPriority((byte)3);
      messages[7] = createTextMessage("h", session);
      messages[7].setPriority((byte)3);
      messages[8] = createTextMessage("i", session);
      messages[8].setPriority((byte)3);

      messages[9] = createTextMessage("j", session);
      messages[9].setPriority((byte)9);

      for (int i = 0; i < 10; i++)
      {
         producer.send(messages[i]);
      }

      ClientConsumer consumer = session.createConsumer(queue);

      session.start();

      // 1 message with priority 9
      expectMessage((byte)9, "j", consumer);
      // 3 messages with priority 7
      expectMessage((byte)7, "d", consumer);
      expectMessage((byte)7, "e", consumer);
      expectMessage((byte)7, "f", consumer);
      // 3 messages with priority 3
      expectMessage((byte)3, "g", consumer);
      expectMessage((byte)3, "h", consumer);
      expectMessage((byte)3, "i", consumer);
      // 3 messages with priority 0
      expectMessage((byte)0, "a", consumer);
      expectMessage((byte)0, "b", consumer);
      expectMessage((byte)0, "c", consumer);

      consumer.close();
      session.deleteQueue(queue);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      server = Messaging.newNullStorageMessagingServer(config);
      server.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private static void expectMessage(byte expectedPriority, String expectedStringInBody, ClientConsumer consumer) throws Exception
   {
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(expectedPriority, m.getPriority());
      assertEquals(expectedStringInBody, m.getBody().readString());
   }

   // Inner classes -------------------------------------------------

}
