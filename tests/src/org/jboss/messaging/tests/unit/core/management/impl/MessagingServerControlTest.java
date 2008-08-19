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
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.management.impl.MessagingServerControl.NotificationType;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.version.Version;
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
   private PostOffice postOffice;
   private StorageManager storageManager;
   private Configuration configuration;
   private HierarchicalRepository<Set<Role>> securityRepository;
   private HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private MessagingServer server;

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

      expect(server.isStarted()).andStubReturn(started);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(started, control.isStarted());

      verifyMocks();
   }

   public void testGetVersion() throws Exception
   {

      String fullVersion = randomString();
      Version version = createMock(Version.class);
      expect(version.getFullVersion()).andReturn(fullVersion);
      expect(server.getVersion()).andStubReturn(version);
      replayMocks();
      replay(version);

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(fullVersion, control.getVersion());

      verify(version);
      verifyMocks();
   }

   public void testGetBindingsDirectory() throws Exception
   {
      String dir = randomString();

      expect(configuration.getBindingsDirectory()).andReturn(dir);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(dir, control.getBindingsDirectory());

      verifyMocks();
   }

   public void testGetInterceptorClassNames() throws Exception
   {
      List<String> list = new ArrayList<String>();
      list.add(randomString());

      expect(configuration.getInterceptorClassNames()).andReturn(list);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(list, control.getInterceptorClassNames());

      verifyMocks();
   }

   public void testGetJournalDirectory() throws Exception
   {
      String dir = randomString();

      expect(configuration.getJournalDirectory()).andReturn(dir);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(dir, control.getJournalDirectory());

      verifyMocks();
   }

   public void testGetJournalFileSize() throws Exception
   {
      int size = randomInt();

      expect(configuration.getJournalFileSize()).andReturn(size);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(size, control.getJournalFileSize());

      verifyMocks();
   }

   public void testGetJournalMaxAIO() throws Exception
   {
      int max = randomInt();

      expect(configuration.getJournalMaxAIO()).andReturn(max);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(max, control.getJournalMaxAIO());

      verifyMocks();
   }

   public void testGetJournalMinFiles() throws Exception
   {
      int minFiles = randomInt();

      expect(configuration.getJournalMinFiles()).andReturn(minFiles);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(minFiles, control.getJournalMinFiles());

      verifyMocks();
   }

   public void testGetJournalType() throws Exception
   {
      expect(configuration.getJournalType()).andReturn(JournalType.ASYNCIO);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(JournalType.ASYNCIO.toString(), control.getJournalType());

      verifyMocks();
   }

   public void testGetKeyStorePath() throws Exception
   {
      String path = randomString();

      expect(configuration.getKeyStorePath()).andReturn(path);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(path, control.getKeyStorePath());

      verifyMocks();
   }

   public void testGetLocation() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost");

      expect(configuration.getLocation()).andReturn(location);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(location.toString(), control.getLocation());

      verifyMocks();
   }

   public void testGetScheduledThreadPoolMaxSize() throws Exception
   {
      int size = randomInt();

      expect(configuration.getScheduledThreadPoolMaxSize()).andReturn(size);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(size, control.getScheduledThreadPoolMaxSize());

      verifyMocks();
   }

   public void testGetSecurityInvalidationInterval() throws Exception
   {
      long interval = randomLong();

      expect(configuration.getSecurityInvalidationInterval()).andReturn(
            interval);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(interval, control.getSecurityInvalidationInterval());

      verifyMocks();
   }

   public void testGetTrustStorePath() throws Exception
   {
      String path = randomString();

      expect(configuration.getTrustStorePath()).andReturn(path);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(path, control.getTrustStorePath());

      verifyMocks();
   }

   public void testIsClustered() throws Exception
   {
      boolean clustered = randomBoolean();

      expect(configuration.isClustered()).andReturn(clustered);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(clustered, control.isClustered());

      verifyMocks();
   }

   public void testIsCreateBindingsDir() throws Exception
   {
      boolean createBindingsDir = randomBoolean();

      expect(configuration.isCreateBindingsDir()).andReturn(createBindingsDir);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(createBindingsDir, control.isCreateBindingsDir());

      verifyMocks();
   }

   public void testIsCreateJournalDir() throws Exception
   {
      boolean createJournalDir = randomBoolean();

      expect(configuration.isCreateJournalDir()).andReturn(createJournalDir);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(createJournalDir, control.isCreateJournalDir());

      verifyMocks();
   }

   public void testIsJournalSyncNonTransactional() throws Exception
   {
      boolean journalSyncNonTransactional = randomBoolean();

      expect(configuration.isJournalSyncNonTransactional()).andReturn(
            journalSyncNonTransactional);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(journalSyncNonTransactional, control
            .isJournalSyncNonTransactional());

      verifyMocks();
   }

   public void testIsJournalSyncTransactional() throws Exception
   {
      boolean journalSyncTransactional = randomBoolean();

      expect(configuration.isJournalSyncTransactional()).andReturn(
            journalSyncTransactional);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(journalSyncTransactional, control
            .isJournalSyncTransactional());

      verifyMocks();
   }

   public void testIsRequireDestinations() throws Exception
   {
      boolean requireDestinations = randomBoolean();

      expect(configuration.isRequireDestinations()).andReturn(
            requireDestinations);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(requireDestinations, control.isRequireDestinations());

      verifyMocks();
   }

   public void testIsSSLEnabled() throws Exception
   {
      boolean sslEnabled = randomBoolean();

      expect(configuration.isSSLEnabled()).andReturn(sslEnabled);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(sslEnabled, control.isSSLEnabled());

      verifyMocks();
   }

   public void testIsSecurityEnabled() throws Exception
   {
      boolean securityEnabled = randomBoolean();

      expect(configuration.isSecurityEnabled()).andReturn(securityEnabled);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(securityEnabled, control.isSecurityEnabled());

      verifyMocks();
   }

   public void testAddDestination() throws Exception
   {
      String address = randomString();
      boolean added = randomBoolean();

      expect(postOffice.addDestination(new SimpleString(address), false))
            .andReturn(added);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(added, control.addAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_ADDED, listener,
            latch);

      verifyMocks();

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testRemoveAddress() throws Exception
   {
      String address = randomString();
      boolean removed = randomBoolean();

      expect(postOffice.removeDestination(new SimpleString(address), false))
            .andReturn(removed);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(removed, control.removeAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_REMOVED, listener,
            latch);

      verifyMocks();

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testCreateQueue() throws Exception
   {
      String address = randomString();
      String name = randomString();

      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
      Binding newBinding = createMock(Binding.class);
      expect(
            postOffice.addBinding(new SimpleString(address), new SimpleString(
                  name), null, true)).andReturn(newBinding);
      replayMocks();
      replay(newBinding);

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      control.createQueue(address, name);

      verifyMocks();
      verify(newBinding);
   }

   public void testCreateQueueAndReceiveNotification() throws Exception
   {
      String address = randomString();
      String name = randomString();

      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
      Binding newBinding = createMock(Binding.class);
      expect(
            postOffice.addBinding(new SimpleString(address), new SimpleString(
                  name), null, true)).andReturn(newBinding);
      replayMocks();
      replay(newBinding);

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);

      mbeanServer.addNotificationListener(serverON, listener, null, null);
      control.createQueue(address, name);

      assertGotNotification(
            MessagingServerControl.NotificationType.QUEUE_CREATED, listener,
            latch);

      mbeanServer.removeNotificationListener(serverON, listener);

      verify(newBinding);
      verifyMocks();
   }

   public void testCreateQueueWithAllParameters() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = "color = 'green'";
      boolean durable = true;

      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
      Binding newBinding = createMock(Binding.class);
      expect(
            postOffice.addBinding(eq(new SimpleString(address)),
                  eq(new SimpleString(name)), isA(Filter.class), eq(durable)
                  )).andReturn(newBinding);
      replayMocks();
      replay(newBinding);

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMocks();
   }

   public void testCreateQueueWithEmptyFilter() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = "";
      boolean durable = true;
 
      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
      Binding newBinding = createMock(Binding.class);
      expect(
            postOffice.addBinding(new SimpleString(address), new SimpleString(
                  name), null, durable)).andReturn(newBinding);
      replay(newBinding);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMocks();
   }

   public void testCreateQueueWithNullFilter() throws Exception
   {
      String address = randomString();
      String name = randomString();
      String filter = null;
      boolean durable = true;

      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
      Binding newBinding = createMock(Binding.class);
      expect(
            postOffice.addBinding(new SimpleString(address), new SimpleString(
                  name), null, durable)).andReturn(newBinding);
      replay(newBinding);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMocks();
   }

   public void testDestroyQueueAndReceiveNotification() throws Exception
   {
      String name = randomString();

      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(queue.getName()).andReturn(new SimpleString(name));
      expect(binding.getQueue()).andReturn(queue);
      expect(postOffice.getBinding(new SimpleString(name))).andReturn(binding);
      queue.deleteAllReferences(storageManager);
      expect(postOffice.removeBinding(new SimpleString(name))).andReturn(
            binding);
      replayMocks();
      replay(binding, queue);

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);

      mbeanServer.addNotificationListener(serverON, listener, null, null);
      control.destroyQueue(name);

      assertGotNotification(
            MessagingServerControl.NotificationType.QUEUE_DESTROYED, listener,
            latch);

      verify(binding, queue);
      verifyMocks();

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testGetConnectionCount() throws Exception
   {
      int count = randomInt();

      expect(server.getConnectionCount()).andReturn(count);
      replayMocks();

      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
      assertEquals(count, control.getConnectionCount());

      verifyMocks();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = ManagementFactory.getPlatformMBeanServer();
      serverON = ManagementServiceImpl.getMessagingServerObjectName();

      postOffice = createMock(PostOffice.class);
      storageManager = createMock(StorageManager.class);
      configuration = createMock(Configuration.class);
      securityRepository = createMock(HierarchicalRepository.class);
      queueSettingsRepository = createMock(HierarchicalRepository.class);
      server = createMock(MessagingServer.class);
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

      postOffice = null;
      storageManager = null;
      configuration = null;
      securityRepository = null;
      queueSettingsRepository = null;
      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void replayMocks()
   {
      replay(postOffice, storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
   }

   private void verifyMocks()
   {
      verify(postOffice, storageManager, configuration, securityRepository,
            queueSettingsRepository, server);
   }

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
