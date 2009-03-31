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
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

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

   private MessagingServer server;

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
      catch (MessagingException e)
      {
         assertEquals(MessagingException.QUEUE_DOES_NOT_EXIST, e.getCode());
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
      assertNull(consumer.receive(500));

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
      
      expectMessagingException(MessagingException.QUEUE_DOES_NOT_EXIST, new MessagingAction()
      {
         public void run() throws MessagingException
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
      expectMessagingException(MessagingException.QUEUE_DOES_NOT_EXIST, new MessagingAction()
      {
         public void run() throws MessagingException
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
      session.close();

      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void restart() throws Exception
   {
      session.close();

      server.stop();
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
   }
   // Inner classes -------------------------------------------------

}
