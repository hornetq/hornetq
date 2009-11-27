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

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * A MessagePriorityTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class MessagePriorityTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;
   
   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMessagePriority() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < 10; i++)
      {
         ClientMessage m = createTextMessage(Integer.toString(i), session);
         m.setPriority((byte)i);         
         producer.send(m);
      }

      ClientConsumer consumer = session.createConsumer(queue);

      session.start();

      // expect to consumer message with higher priority first
      for (int i = 9; i >= 0; i--)
      {
         ClientMessage m = consumer.receive(500);
         assertNotNull(m);
         assertEquals(i, m.getPriority());
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


      for (int i = 0; i < 10; i++)
      {
         ClientMessage m = createTextMessage(Integer.toString(i), session);
         m.setPriority((byte)i);
         producer.send(m);
      }

      // expect to consumer message with higher priority first
      for (int i = 9; i >= 0; i--)
      {
         ClientMessage m = consumer.receive(500);
         assertNotNull(m);
         assertEquals(i, m.getPriority());         
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
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = HornetQ.newHornetQServer(config, false);
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      sf.close();
      
      session.close();

      server.stop();
      
      sf = null;
      
      session = null;
      
      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private static void expectMessage(byte expectedPriority, String expectedStringInBody, ClientConsumer consumer) throws Exception
   {
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(expectedPriority, m.getPriority());
      assertEquals(expectedStringInBody, m.getBodyBuffer().readString());
   }

   // Inner classes -------------------------------------------------

}
