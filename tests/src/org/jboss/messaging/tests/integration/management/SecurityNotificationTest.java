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

import static org.jboss.messaging.core.client.management.impl.ManagementHelper.HDR_NOTIFICATION_TYPE;
import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
import static org.jboss.messaging.core.management.NotificationType.SECURITY_AUTHENTICATION_VIOLATION;
import static org.jboss.messaging.core.management.NotificationType.SECURITY_PERMISSION_VIOLATION;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMUpdateableSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

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

   private MessagingService service;
   private ClientSession adminSession;
   private ClientConsumer notifConsumer;
   private SimpleString notifQueue;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
  
   public void testSECURITY_AUTHENTICATION_VIOLATION() throws Exception
   {
      String unknownUser = randomString();
 
      flush(notifConsumer);

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      
      try
      {
         sf.createSession(unknownUser, randomString(), false, true, true, false, 1);
         fail("authentication must fail and a notification of security violation must be sent");
      }
      catch (Exception e)
      {
      }
      
      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(SECURITY_AUTHENTICATION_VIOLATION.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals(unknownUser, notifications[0].getProperty(ManagementHelper.HDR_USER).toString());
   }

   public void testSECURITY_PERMISSION_VIOLATION() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      // guest can not create queue
      Role role = new Role("roleCanNotCreateQueue", true, true, false, true, false, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      service.getServer().getSecurityRepository().addMatch(address.toString(), roles);
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) service.getServer().getSecurityManager();
      securityManager.addRole("guest", "roleCanNotCreateQueue");
      
      flush(notifConsumer);

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      ClientSession guestSession = sf.createSession("guest", "guest", false, true, true, false, 1);

      try
      {
         guestSession.createQueue(address, queue, true, false);
         fail("session creation must fail and a notification of security violation must be sent");
      }
      catch (Exception e)
      {
      }
      
      ClientMessage[] notifications = consumeMessages(1, notifConsumer);
      assertEquals(SECURITY_PERMISSION_VIOLATION.toString(), notifications[0].getProperty(HDR_NOTIFICATION_TYPE).toString());
      assertEquals("guest", notifications[0].getProperty(ManagementHelper.HDR_USER).toString());
      assertEquals(address.toString(), notifications[0].getProperty(ManagementHelper.HDR_ADDRESS).toString());
      assertEquals(CheckType.CREATE_DURABLE_QUEUE.toString(), notifications[0].getProperty(ManagementHelper.HDR_CHECK_TYPE).toString());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(true);
      // the notifications are independent of JMX
      conf.setJMXManagementEnabled(false);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newNullStorageMessagingService(conf);
      service.start();

      notifQueue = randomSimpleString();

      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) service.getServer().getSecurityManager();
      securityManager.addUser("admin", "admin");      
      securityManager.addUser("guest", "guest");
      securityManager.setDefaultUser("guest");

      Role role = new Role("notif", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      service.getServer().getSecurityRepository().addMatch(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS.toString(), roles);

      securityManager.addRole("admin", "notif");

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      adminSession = sf.createSession("admin", "admin", false, true, true, false, 1);
      adminSession.start();
      
      adminSession.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueue, null, false, true);

      notifConsumer = adminSession.createConsumer(notifQueue);
   }

   @Override
   protected void tearDown() throws Exception
   {
      notifConsumer.close();
      
      adminSession.deleteQueue(notifQueue);
      adminSession.close();
      
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
