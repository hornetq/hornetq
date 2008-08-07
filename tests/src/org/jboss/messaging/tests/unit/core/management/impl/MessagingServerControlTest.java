/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.management.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.management.impl.MessagingServerControl.NotificationType;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.util.SimpleString;

/**
 * t
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingServerControlTest extends TestCase
{
   private MBeanServer mbeanServer;
   private ObjectName serverON;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void assertGotNotification(NotificationType expectedType,
         NotificationListenerWithLatch listener, CountDownLatch latch)
         throws Exception
   {
      boolean gotNotification = latch.await(500, MILLISECONDS);
      assertTrue(gotNotification);
      Notification notification = listener.getNotification();
      assertNotNull(notification);
      assertEquals(expectedType.toString(), notification.getType());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsStarted() throws Exception
   {
      boolean started = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.isStarted()).andStubReturn(started);
      Configuration configuration = createMock(Configuration.class);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(started, control.isStarted());

      verify(server, configuration);
   }

   public void testGetVersion() throws Exception
   {
      String version = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.getVersion()).andStubReturn(version);
      Configuration configuration = createMock(Configuration.class);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(version, control.getVersion());

      verify(server, configuration);
   }

   public void testGetBindingsDirectory() throws Exception
   {
      String dir = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getBindingsDirectory()).andReturn(dir);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(dir, control.getBindingsDirectory());

      verify(server, configuration);
   }

   public void testGetInterceptorClassNames() throws Exception
   {
      List<String> list = new ArrayList<String>();
      list.add(randomString());

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getInterceptorClassNames()).andReturn(list);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(list, control.getInterceptorClassNames());

      verify(server, configuration);
   }

   public void testGetJournalDirectory() throws Exception
   {
      String dir = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getJournalDirectory()).andReturn(dir);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(dir, control.getJournalDirectory());

      verify(server, configuration);
   }

   public void testGetJournalFileSize() throws Exception
   {
      int size = randomInt();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getJournalFileSize()).andReturn(size);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(size, control.getJournalFileSize());

      verify(server, configuration);
   }

   public void testGetJournalMaxAIO() throws Exception
   {
      int max = randomInt();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getJournalMaxAIO()).andReturn(max);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(max, control.getJournalMaxAIO());

      verify(server, configuration);
   }

   public void testGetJournalMinFiles() throws Exception
   {
      int minFiles = randomInt();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getJournalMinFiles()).andReturn(minFiles);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(minFiles, control.getJournalMinFiles());

      verify(server, configuration);
   }

   public void testGetJournalType() throws Exception
   {
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getJournalType()).andReturn(JournalType.ASYNCIO);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(JournalType.ASYNCIO.toString(), control.getJournalType());

      verify(server, configuration);
   }

   public void testGetKeyStorePath() throws Exception
   {
      String path = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getKeyStorePath()).andReturn(path);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(path, control.getKeyStorePath());

      verify(server, configuration);
   }

   public void testGetLocation() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost");

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getLocation()).andReturn(location);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(location.toString(), control.getLocation());

      verify(server, configuration);
   }

   public void testGetScheduledThreadPoolMaxSize() throws Exception
   {
      int size = randomInt();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getScheduledThreadPoolMaxSize()).andReturn(size);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(size, control.getScheduledThreadPoolMaxSize());

      verify(server, configuration);
   }

   public void testGetSecurityInvalidationInterval() throws Exception
   {
      long interval = randomLong();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getSecurityInvalidationInterval()).andReturn(
            interval);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(interval, control.getSecurityInvalidationInterval());

      verify(server, configuration);
   }

   public void testGetTrustStorePath() throws Exception
   {
      String path = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.getTrustStorePath()).andReturn(path);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(path, control.getTrustStorePath());

      verify(server, configuration);
   }

   public void testIsClustered() throws Exception
   {
      boolean clustered = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isClustered()).andReturn(clustered);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(clustered, control.isClustered());

      verify(server, configuration);
   }

   public void testIsCreateBindingsDir() throws Exception
   {
      boolean createBindingsDir = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isCreateBindingsDir()).andReturn(createBindingsDir);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(createBindingsDir, control.isCreateBindingsDir());

      verify(server, configuration);
   }

   public void testIsCreateJournalDir() throws Exception
   {
      boolean createJournalDir = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isCreateJournalDir()).andReturn(createJournalDir);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(createJournalDir, control.isCreateJournalDir());

      verify(server, configuration);
   }

   public void testIsJournalSyncNonTransactional() throws Exception
   {
      boolean journalSyncNonTransactional = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isJournalSyncNonTransactional()).andReturn(
            journalSyncNonTransactional);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(journalSyncNonTransactional, control
            .isJournalSyncNonTransactional());

      verify(server, configuration);
   }

   public void testIsJournalSyncTransactional() throws Exception
   {
      boolean journalSyncTransactional = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isJournalSyncTransactional()).andReturn(
            journalSyncTransactional);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(journalSyncTransactional, control
            .isJournalSyncTransactional());

      verify(server, configuration);
   }

   public void testIsRequireDestinations() throws Exception
   {
      boolean requireDestinations = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isRequireDestinations()).andReturn(
            requireDestinations);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(requireDestinations, control.isRequireDestinations());

      verify(server, configuration);
   }

   public void testIsSSLEnabled() throws Exception
   {
      boolean sslEnabled = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isSSLEnabled()).andReturn(sslEnabled);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(sslEnabled, control.isSSLEnabled());

      verify(server, configuration);
   }

   public void testIsSecurityEnabled() throws Exception
   {
      boolean securityEnabled = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(configuration.isSecurityEnabled()).andReturn(securityEnabled);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(securityEnabled, control.isSecurityEnabled());

      verify(server, configuration);
   }

   public void testAddDestination() throws Exception
   {
      String address = randomString();
      boolean added = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.addDestination(new SimpleString(address))).andReturn(added);
      Configuration configuration = createMock(Configuration.class);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(added, control.addAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_ADDED, listener,
            latch);

      verify(server, configuration);

      mbeanServer.removeNotificationListener(serverON, listener);

      verify(server, configuration);
   }

   public void testRemoveAddress() throws Exception
   {
      String address = randomString();
      boolean removed = randomBoolean();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.removeDestination(new SimpleString(address))).andReturn(
            removed);
      Configuration configuration = createMock(Configuration.class);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(removed, control.removeAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_REMOVED, listener,
            latch);

      verify(server, configuration);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testCreateQueue() throws Exception
   {
      String address = randomString();
      String name = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.createQueue(new SimpleString(address), new SimpleString(name));
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      control.createQueue(address, name);

      verify(server, configuration);
   }

   public void testCreateQueueAndReceiveNotification() throws Exception
   {
      String address = randomString();
      String name = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.createQueue(new SimpleString(address), new SimpleString(name));
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);

      mbeanServer.addNotificationListener(serverON, listener, null, null);
      control.createQueue(address, name);

      assertGotNotification(
            MessagingServerControl.NotificationType.QUEUE_CREATED, listener,
            latch);

      verify(server, configuration);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testCreateQueueWithAllParameters() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = "color = 'green'";
      boolean durable = true;
      boolean temporary = false;

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.createQueue(new SimpleString(address), new SimpleString(name),
            new SimpleString(filter), durable, temporary);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      control.createQueue(address, name, filter, durable, temporary);

      verify(server, configuration);
   }

   public void testCreateQueueWithEmptyFilter() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = "";
      boolean durable = true;
      boolean temporary = false;

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.createQueue(new SimpleString(address), new SimpleString(name),
            null, durable, temporary);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      control.createQueue(address, name, filter, durable, temporary);

      verify(server, configuration);
   }

   public void testCreateQueueWithNullFilter() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = null;
      boolean durable = true;
      boolean temporary = false;

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.createQueue(new SimpleString(address), new SimpleString(name),
            null, durable, temporary);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      control.createQueue(address, name, filter, durable, temporary);

      verify(server, configuration);
   }

   public void testCreateQueueWithBothDurableAndTemporarySetToTrueFails()
         throws Exception
   {
      String address = randomString();
      String name = randomString();
      boolean durable = true;
      boolean temporary = true;

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      try
      {
         control.createQueue(address, name, null, durable, temporary);
         fail("a queue can not be both durable and temporary");
      } catch (IllegalArgumentException e)
      {
      }

      verify(server, configuration);
   }

   public void testDestroyQueueAndReceiveNotification() throws Exception
   {
      String name = randomString();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      server.destroyQueue(new SimpleString(name));
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);

      mbeanServer.addNotificationListener(serverON, listener, null, null);
      control.destroyQueue(name);

      assertGotNotification(
            MessagingServerControl.NotificationType.QUEUE_DESTROYED, listener,
            latch);

      verify(server, configuration);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testGetConnectionCount() throws Exception
   {
      int count = randomInt();

      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Configuration configuration = createMock(Configuration.class);
      expect(server.getConnectionCount()).andReturn(count);
      replay(server, configuration);

      MessagingServerControl control = new MessagingServerControl(server,
            configuration);
      assertEquals(count, control.getConnectionCount());

      verify(server, configuration);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = ManagementFactory.getPlatformMBeanServer();
      serverON = ManagementServiceImpl.getMessagingServerObjectName();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (mbeanServer.isRegistered(serverON))
      {
         mbeanServer.unregisterMBean(serverON);
      }

      serverON = null;
      mbeanServer = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class NotificationListenerWithLatch implements
         NotificationListener
   {
      private final CountDownLatch latch;
      private Notification notification;

      private NotificationListenerWithLatch(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void handleNotification(Notification notification, Object handback)
      {
         this.notification = notification;
         latch.countDown();
      }

      public Notification getNotification()
      {
         return notification;
      }
   }

}
