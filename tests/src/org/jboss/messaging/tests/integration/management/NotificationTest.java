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

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import java.util.Set;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.util.SimpleString;

/**
 * A NotificationTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class NotificationTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNotification() throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      
      ClientSession session = sf.createSession(false, true, true);

      // create a queue to receive the management notifications
      SimpleString notifQueue = randomSimpleString();
      session.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueue, null, false, true, true);
      ClientConsumer notifConsumer = session.createConsumer(notifQueue);
      session.start();
      
      // generate notifications
      SimpleString destinationName = randomSimpleString();
      session.addDestination(destinationName, false, true);
      session.removeDestination(destinationName, false);

      // we've generated at least 2 notifications
      // but there is more in the queue (e.g. the notification when the notifQueue was created)      
      ClientMessage notifMessage = notifConsumer.receive(500);
      assertNotNull(notifMessage);
      Set<SimpleString> propertyNames = notifMessage.getPropertyNames();

      for (SimpleString key : propertyNames)
      {
         System.out.println(key + "=" + notifMessage.getProperty(key));
      }

      notifMessage.acknowledge();
      

      notifMessage = notifConsumer.receive(500);
      assertNotNull(notifMessage);
      propertyNames = notifMessage.getPropertyNames();
      for (SimpleString key : propertyNames)
      {
         System.out.println(key + "=" + notifMessage.getProperty(key));
      }
      notifMessage.acknowledge();
      
      notifConsumer.close();
      session.deleteQueue(notifQueue);
      session.close();
   }
   
   public void testNotificationWithFilter() throws Exception
   {
      SimpleString destinationName = randomSimpleString();
      SimpleString unmatchedDestinationName = new SimpleString("this.destination.does.not.match.the.filter");

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      
      ClientSession session = sf.createSession(false, true, true);

      // create a queue to receive the management notifications only concerning the destination
      SimpleString notifQueue = randomSimpleString();
      SimpleString filter = new SimpleString(ManagementHelper.HDR_NOTIFICATION_MESSAGE + " LIKE '%" + destinationName + "%'" );
      System.out.println(filter);
      session.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueue, filter, false, true, true);
      ClientConsumer notifConsumer = session.createConsumer(notifQueue);
      session.start();

      // generate notifications that do NOT match the filter
      session.addDestination(unmatchedDestinationName, false, true);
      session.removeDestination(unmatchedDestinationName, false);
      
      assertNull(notifConsumer.receive(500));
      
      // generate notifications that match the filter
      session.addDestination(destinationName, false, true);
      session.removeDestination(destinationName, false);

      ClientMessage notifMessage = notifConsumer.receive(500);
      assertNotNull(notifMessage);
      assertEquals(NotificationType.ADDRESS_ADDED.toString(), notifMessage.getProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Set<SimpleString> propertyNames = notifMessage.getPropertyNames();

      for (SimpleString key : propertyNames)
      {
         System.out.println(key + "=" + notifMessage.getProperty(key));
      }

      notifMessage.acknowledge();
      

      notifMessage = notifConsumer.receive(500);
      assertNotNull(notifMessage);
      assertEquals(NotificationType.ADDRESS_REMOVED.toString(), notifMessage.getProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      propertyNames = notifMessage.getPropertyNames();
      for (SimpleString key : propertyNames)
      {
         System.out.println(key + "=" + notifMessage.getProperty(key));
      }
      notifMessage.acknowledge();
      
      // no other notifications matching the filter
      assertNull(notifConsumer.receive(500));
      
      notifConsumer.close();
      session.deleteQueue(notifQueue);
      session.close();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      // the notifications are independent of JMX
      conf.setJMXManagementEnabled(false);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
