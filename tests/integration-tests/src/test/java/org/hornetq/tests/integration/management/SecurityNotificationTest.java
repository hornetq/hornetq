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

import static org.hornetq.api.core.management.NotificationType.SECURITY_AUTHENTICATION_VIOLATION;
import static org.hornetq.api.core.management.NotificationType.SECURITY_PERMISSION_VIOLATION;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A SecurityNotificationTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class SecurityNotificationTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession adminSession;

   private ClientConsumer notifConsumer;

   private SimpleString notifQueue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testSECURITY_AUTHENTICATION_VIOLATION() throws Exception
   {
      String unknownUser = RandomUtil.randomString();

      SecurityNotificationTest.flush(notifConsumer);

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = createSessionFactory(locator);

      try
      {
         sf.createSession(unknownUser, RandomUtil.randomString(), false, true, true, false, 1);
         Assert.fail("authentication must fail and a notification of security violation must be sent");
      }
      catch (Exception e)
      {
      }

      ClientMessage[] notifications = SecurityNotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(SECURITY_AUTHENTICATION_VIOLATION.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(unknownUser, notifications[0].getObjectProperty(ManagementHelper.HDR_USER).toString());
   }

   @Test
   public void testSECURITY_PERMISSION_VIOLATION() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      // guest can not create queue
      Role role = new Role("roleCanNotCreateQueue", true, true, false, true, false, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), roles);
      HornetQSecurityManager securityManager = server.getSecurityManager();
      securityManager.addRole("guest", "roleCanNotCreateQueue");

      SecurityNotificationTest.flush(notifConsumer);

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession guestSession = sf.createSession("guest", "guest", false, true, true, false, 1);

      try
      {
         guestSession.createQueue(address, queue, true);
         Assert.fail("session creation must fail and a notification of security violation must be sent");
      }
      catch (Exception e)
      {
      }

      ClientMessage[] notifications = SecurityNotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(SECURITY_PERMISSION_VIOLATION.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals("guest", notifications[0].getObjectProperty(ManagementHelper.HDR_USER).toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS)
                                                              .toString());
      Assert.assertEquals(CheckType.CREATE_DURABLE_QUEUE.toString(),
                          notifications[0].getObjectProperty(ManagementHelper.HDR_CHECK_TYPE).toString());

      guestSession.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(true);
      // the notifications are independent of JMX
      conf.setJMXManagementEnabled(false);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      notifQueue = RandomUtil.randomSimpleString();

      HornetQSecurityManager securityManager = server.getSecurityManager();
      securityManager.addUser("admin", "admin");
      securityManager.addUser("guest", "guest");
      securityManager.setDefaultUser("guest");

      Role role = new Role("notif", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch(HornetQDefaultConfiguration.getDefaultManagementNotificationAddress().toString(),
                                              roles);

      securityManager.addRole("admin", "notif");

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = createSessionFactory(locator);
      adminSession = sf.createSession("admin", "admin", false, true, true, false, 1);
      adminSession.start();

      adminSession.createTemporaryQueue(HornetQDefaultConfiguration.getDefaultManagementNotificationAddress(), notifQueue);

      notifConsumer = adminSession.createConsumer(notifQueue);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      notifConsumer.close();

      adminSession.deleteQueue(notifQueue);
      adminSession.close();

      server.stop();

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
