/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ConsumerCloseTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConsumerCloseTest.class);


   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientSession session;

   private SimpleString queue;

   private SimpleString address;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCanNotUseAClosedConsumer() throws Exception
   {
      final ClientConsumer consumer = session.createConsumer(queue);

      consumer.close();

      assertTrue(consumer.isClosed());

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            consumer.receive();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            consumer.receiveImmediate();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            consumer.setMessageHandler(new MessageHandler()
            {
               public void onMessage(ClientMessage message)
               {
               }
            });
         }
      });
   }

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1526
   public void testCloseWithManyMessagesInBufferAndSlowConsumer() throws Exception
   {
      ClientConsumer consumer = session.createConsumer(queue);

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(false);

         producer.send(message);
      }

      class MyHandler implements MessageHandler
      {
         public void onMessage(ClientMessage message)
         {
            try
            {
               Thread.sleep(1000);
            }
            catch (Exception e)
            {
            }
         }
      }
      
      consumer.setMessageHandler(new MyHandler());
      
      session.start();
      
      Thread.sleep(1000);
      
      //Close shouldn't wait for all messages to be processed before closing
      long start= System.currentTimeMillis();
      consumer.close();
      long end = System.currentTimeMillis();
      
      assertTrue(end - start <= 1500);

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
      server = Messaging.newMessagingServer(config, false);
      server.start();

      address = randomSimpleString();
      queue = randomSimpleString();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.createQueue(address, queue, false);

   }

   private ClientSessionFactory sf;

   @Override
   protected void tearDown() throws Exception
   {
      session.deleteQueue(queue);

      session.close();

      sf.close();

      server.stop();
      
      session = null;
      sf = null;
      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
