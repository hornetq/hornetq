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


package org.hornetq.tests.integration.management;

import static org.hornetq.core.client.management.impl.ManagementHelper.HDR_ADDRESS;
import static org.hornetq.core.client.management.impl.ManagementHelper.HDR_CONSUMER_COUNT;
import static org.hornetq.core.client.management.impl.ManagementHelper.HDR_NOTIFICATION_TYPE;
import static org.hornetq.core.client.management.impl.ManagementHelper.HDR_ROUTING_NAME;
import static org.hornetq.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
import static org.hornetq.core.management.NotificationType.BINDING_ADDED;
import static org.hornetq.core.management.NotificationType.BINDING_REMOVED;
import static org.hornetq.core.management.NotificationType.CONSUMER_CLOSED;
import static org.hornetq.core.management.NotificationType.CONSUMER_CREATED;
import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   private HornetQServer service;
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
      service = HornetQ.newMessagingServer(conf, false);
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

   
   private static void flush(ClientConsumer notifConsumer) throws HornetQException
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
