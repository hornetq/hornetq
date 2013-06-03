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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import static org.hornetq.api.core.management.NotificationType.BINDING_ADDED;
import static org.hornetq.api.core.management.NotificationType.BINDING_REMOVED;
import static org.hornetq.api.core.management.NotificationType.CONSUMER_CLOSED;
import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

import org.junit.Assert;
import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

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
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testBINDING_ADDED() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(BINDING_ADDED.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME)
                                                            .toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_ADDEDWithMatchingFilter() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), ManagementHelper.HDR_ROUTING_NAME + "= '" +
                                                                    queue +
                                                                    "'");
      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(BINDING_ADDED.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME)
                                                            .toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_ADDEDWithNonMatchingFilter() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), ManagementHelper.HDR_ROUTING_NAME + " <> '" +
                                                                    queue +
                                                                    "'");
      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      NotificationTest.consumeMessages(0, notifConsumer);

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_REMOVED() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      NotificationTest.flush(notifConsumer);

      session.deleteQueue(queue);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(BINDING_REMOVED.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME)
                                                            .toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());
   }

   @Test
   public void testCONSUMER_CREATED() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser",
            "myPassword",
            false,
            true,
            true,
            locator.isPreAcknowledge(),
            locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      NotificationTest.flush(notifConsumer);

      ClientConsumer consumer = mySession.createConsumer(queue);
      SimpleString consumerName = SimpleString.toSimpleString(((ClientSessionInternal) mySession).getName());

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(CONSUMER_CREATED.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME)
                                                            .toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());
      Assert.assertEquals(1, notifications[0].getObjectProperty(ManagementHelper.HDR_CONSUMER_COUNT));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_USER));
      Assert.assertEquals(SimpleString.toSimpleString("invm:0"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS));
      Assert.assertEquals(consumerName, notifications[0].getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME));

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testCONSUMER_CLOSED() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser",
            "myPassword",
            false,
            true,
            true,
            locator.isPreAcknowledge(),
            locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      mySession.createQueue(address, queue, durable);
      ClientConsumer consumer = mySession.createConsumer(queue);
      SimpleString sessionName = SimpleString.toSimpleString(((ClientSessionInternal) mySession).getName());

      NotificationTest.flush(notifConsumer);

      consumer.close();

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(CONSUMER_CLOSED.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME)
                                                            .toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());
      Assert.assertEquals(0, notifications[0].getObjectProperty(ManagementHelper.HDR_CONSUMER_COUNT));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_USER));
      Assert.assertEquals(SimpleString.toSimpleString("invm:0"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS));
      Assert.assertEquals(sessionName, notifications[0].getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME));

      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      // the notifications are independent of JMX
      conf.setJMXManagementEnabled(false);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = HornetQServers.newHornetQServer(conf, false);
      service.start();

      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();

      notifQueue = RandomUtil.randomSimpleString();

      session.createQueue(HornetQDefaultConfiguration.getDefaultManagementNotificationAddress(), notifQueue, null, false);

      notifConsumer = session.createConsumer(notifQueue);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      notifConsumer.close();

      session.deleteQueue(notifQueue);
      session.close();

      if(locator != null)
      {
         locator.close();
      }

      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private static void flush(final ClientConsumer notifConsumer) throws HornetQException
   {
      ClientMessage message = null;
      do
      {
         message = notifConsumer.receive(500);
      }
      while (message != null);
   }

   protected static ClientMessage[] consumeMessages(final int expected, final ClientConsumer consumer) throws Exception
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
               System.out.println(key + "=" + m.getObjectProperty(key));
            }
         }
         Assert.assertNotNull("expected to received " + expected + " messages, got only " + i, m);
         messages[i] = m;
         m.acknowledge();
      }
      m = consumer.receiveImmediate();
      if (m != null)
      {
         for (SimpleString key : m.getPropertyNames())

         {
            System.out.println(key + "=" + m.getObjectProperty(key));
         }
      }
      Assert.assertNull("received one more message than expected (" + expected + ")", m);

      return messages;
   }

   // Inner classes -------------------------------------------------

}
