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


package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.core.client.management.impl.ManagementHelper.HDR_ADDRESS;
import static org.jboss.messaging.core.client.management.impl.ManagementHelper.HDR_CONSUMER_COUNT;
import static org.jboss.messaging.core.client.management.impl.ManagementHelper.HDR_NOTIFICATION_TYPE;
import static org.jboss.messaging.core.client.management.impl.ManagementHelper.HDR_ROUTING_NAME;
import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
import static org.jboss.messaging.core.management.NotificationType.BINDING_ADDED;
import static org.jboss.messaging.core.management.NotificationType.BINDING_REMOVED;
import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CLOSED;
import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CREATED;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A NotificationTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class NotificationTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer service;
   private ClientSession session;
   private ClientConsumer notifConsumer;
   private SimpleString notifQueue;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testBINDING_ADDED() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();
      
      flush(notifConsumer);
      
      session.createQueue(address, queue, durable);

      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(BINDING_ADDED.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(queue.toString(), notifications[0].getProperty(HDR_ROUTING_NAME).toString());
      assertEquals(address.toString(), notifications[0].getProperty(HDR_ADDRESS).toString());

      session.deleteQueue(queue);
   }
   
   public void testBINDING_ADDEDWithMatchingFilter() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), HDR_ROUTING_NAME + "= '" + queue + "'");
      flush(notifConsumer);
      
      session.createQueue(address, queue, durable);

      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(BINDING_ADDED.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(queue.toString(), notifications[0].getProperty(HDR_ROUTING_NAME).toString());
      assertEquals(address.toString(), notifications[0].getProperty(HDR_ADDRESS).toString());

      session.deleteQueue(queue);
   }
   
   public void testBINDING_ADDEDWithNonMatchingFilter() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), HDR_ROUTING_NAME + " <> '" + queue + "'");
      flush(notifConsumer);
      
      session.createQueue(address, queue, durable);

      consumeMessages(0, notifConsumer);

      session.deleteQueue(queue);
   }
   
   public void testBINDING_REMOVED() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();

      session.createQueue(address, queue, durable);

      flush(notifConsumer);

      session.deleteQueue(queue);

      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(BINDING_REMOVED.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(queue.toString(), notifications[0].getProperty(HDR_ROUTING_NAME).toString());
      assertEquals(address.toString(), notifications[0].getProperty(HDR_ADDRESS).toString());
   }
   
   public void testCONSUMER_CREATED() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();

      session.createQueue(address, queue, durable);

      flush(notifConsumer);

      ClientConsumer consumer = session.createConsumer(queue);
      
      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(CONSUMER_CREATED.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(queue.toString(), notifications[0].getProperty(HDR_ROUTING_NAME).toString());
      assertEquals(address.toString(), notifications[0].getProperty(HDR_ADDRESS).toString());
      assertEquals(1, notifications[0].getProperty(HDR_CONSUMER_COUNT));

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testCONSUMER_CLOSED() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();
      boolean durable = randomBoolean();

      session.createQueue(address, queue, durable);
      ClientConsumer consumer = session.createConsumer(queue);
      
      flush(notifConsumer);

      consumer.close();
      
      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(CONSUMER_CLOSED.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(queue.toString(), notifications[0].getProperty(HDR_ROUTING_NAME).toString());
      assertEquals(address.toString(), notifications[0].getProperty(HDR_ADDRESS).toString());
      assertEquals(0, notifications[0].getProperty(HDR_CONSUMER_COUNT));

      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      // the notifications are independent of JMX
      conf.setJMXManagementEnabled(false);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newMessagingServer(conf, false);
      service.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();
      
      notifQueue = randomSimpleString();
      
      session.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueue, null, false);

      notifConsumer = session.createConsumer(notifQueue);
   }

   @Override
   protected void tearDown() throws Exception
   {
      notifConsumer.close();
      
      session.deleteQueue(notifQueue);
      session.close();
      
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   
   private static void flush(ClientConsumer notifConsumer) throws MessagingException
   {
      ClientMessage message = null;
      do
      {
         message = notifConsumer.receive(500);
      } while (message != null);
   }

   
   protected static ClientMessage[] consumeMessages(int expected, ClientConsumer consumer) throws Exception
   {
      ClientMessage[] messages = new ClientMessage[expected];
      
      ClientMessage m = null;
      for (int i = 0; i < expected; i++)
      {
         m = consumer.receive(500);
         if (m != null)
         {
            for (SimpleString key : m.getPropertyNames())
            {
               System.out.println(key + "=" + m.getProperty(key));
            }    
         }
         assertNotNull("expected to received " + expected + " messages, got only " + i, m);
         messages[i] = m;
         m.acknowledge();
      }
      m = consumer.receive(500);
      if (m != null)
      {
         for (SimpleString key : m.getPropertyNames())

         {
            System.out.println(key + "=" + m.getProperty(key));
         }
      }    
      assertNull("received one more message than expected (" + expected + ")", m);
      
      return messages;
   }
   
   // Inner classes -------------------------------------------------

}
