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
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A MessagDurabilityTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class MessageDurabilityTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNonDurableMessageOnNonDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, !durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(!durable));
      
      restart();

      session.start();
      try
      {
         session.createConsumer(queue);
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.QUEUE_DOES_NOT_EXIST, e.getCode());
      }
   }

   public void testNonDurableMessageOnDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(!durable));

      restart();

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      assertNull(consumer.receiveImmediate());

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testDurableMessageOnDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(durable));

      restart();

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      assertNotNull(consumer.receive(500));

      consumer.close();
      session.deleteQueue(queue);
   }

   /**
    * we can send a durable msg to a non durable queue but the msg won't be persisted
    */
   public void testDurableMessageOnNonDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = randomSimpleString();
      final SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, !durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(durable));

      restart();

      session.start();
      
      expectHornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(queue);
         }
      });
   }

   /**
    * we can send a durable msg to a temp queue but the msg won't be persisted
    */
   public void testDurableMessageOnTemporaryQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = randomSimpleString();
      final SimpleString queue = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(durable));

      restart();

      session.start();
      expectHornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(queue);
         }
      });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(true);
      server.start();

      sf = createInVMFactory();
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      sf.close();
      
      session.close();

      server.stop();
      
      server = null;
      
      session = null;
      
      sf = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void restart() throws Exception
   {
      session.close();

      server.stop();
      server.start();

      sf = createInVMFactory();
      session = sf.createSession(false, true, true);
   }
   // Inner classes -------------------------------------------------

}
