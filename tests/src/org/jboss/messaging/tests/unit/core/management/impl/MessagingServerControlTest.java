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
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
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
   private MessageCounterManager messageCounterManager;

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
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(started, control.isStarted());

      verifyMockedAttributes();
   }

   public void testGetVersion() throws Exception
   {

      String fullVersion = randomString();
      Version version = createMock(Version.class);
      expect(version.getFullVersion()).andReturn(fullVersion);
      expect(server.getVersion()).andStubReturn(version);
      replayMockedAttributes();
      replay(version);

      MessagingServerControl control = createControl();
      assertEquals(fullVersion, control.getVersion());

      verify(version);
      verifyMockedAttributes();
   }

   public void testGetBindingsDirectory() throws Exception
   {
      String dir = randomString();

      expect(configuration.getBindingsDirectory()).andReturn(dir);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(dir, control.getBindingsDirectory());

      verifyMockedAttributes();
   }

   public void testGetInterceptorClassNames() throws Exception
   {
      List<String> list = new ArrayList<String>();
      list.add(randomString());

      expect(configuration.getInterceptorClassNames()).andReturn(list);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(list, control.getInterceptorClassNames());

      verifyMockedAttributes();
   }

   public void testGetJournalDirectory() throws Exception
   {
      String dir = randomString();

      expect(configuration.getJournalDirectory()).andReturn(dir);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(dir, control.getJournalDirectory());

      verifyMockedAttributes();
   }

   public void testGetJournalFileSize() throws Exception
   {
      int size = randomInt();

      expect(configuration.getJournalFileSize()).andReturn(size);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(size, control.getJournalFileSize());

      verifyMockedAttributes();
   }

   public void testGetJournalMaxAIO() throws Exception
   {
      int max = randomInt();

      expect(configuration.getJournalMaxAIO()).andReturn(max);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(max, control.getJournalMaxAIO());

      verifyMockedAttributes();
   }

   public void testGetJournalMinFiles() throws Exception
   {
      int minFiles = randomInt();

      expect(configuration.getJournalMinFiles()).andReturn(minFiles);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(minFiles, control.getJournalMinFiles());

      verifyMockedAttributes();
   }

   public void testGetJournalType() throws Exception
   {
      expect(configuration.getJournalType()).andReturn(JournalType.ASYNCIO);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(JournalType.ASYNCIO.toString(), control.getJournalType());

      verifyMockedAttributes();
   }

   public void testGetKeyStorePath() throws Exception
   {
      String path = randomString();

      expect(configuration.getKeyStorePath()).andReturn(path);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(path, control.getKeyStorePath());

      verifyMockedAttributes();
   }

   public void testGetLocation() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost");

      expect(configuration.getLocation()).andReturn(location);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(location.toString(), control.getLocation());

      verifyMockedAttributes();
   }

   public void testGetScheduledThreadPoolMaxSize() throws Exception
   {
      int size = randomInt();

      expect(configuration.getScheduledThreadPoolMaxSize()).andReturn(size);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(size, control.getScheduledThreadPoolMaxSize());

      verifyMockedAttributes();
   }

   public void testGetSecurityInvalidationInterval() throws Exception
   {
      long interval = randomLong();

      expect(configuration.getSecurityInvalidationInterval()).andReturn(
            interval);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(interval, control.getSecurityInvalidationInterval());

      verifyMockedAttributes();
   }

   public void testGetTrustStorePath() throws Exception
   {
      String path = randomString();

      expect(configuration.getTrustStorePath()).andReturn(path);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(path, control.getTrustStorePath());

      verifyMockedAttributes();
   }

   public void testIsClustered() throws Exception
   {
      boolean clustered = randomBoolean();

      expect(configuration.isClustered()).andReturn(clustered);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(clustered, control.isClustered());

      verifyMockedAttributes();
   }

   public void testIsCreateBindingsDir() throws Exception
   {
      boolean createBindingsDir = randomBoolean();

      expect(configuration.isCreateBindingsDir()).andReturn(createBindingsDir);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(createBindingsDir, control.isCreateBindingsDir());

      verifyMockedAttributes();
   }

   public void testIsCreateJournalDir() throws Exception
   {
      boolean createJournalDir = randomBoolean();

      expect(configuration.isCreateJournalDir()).andReturn(createJournalDir);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(createJournalDir, control.isCreateJournalDir());

      verifyMockedAttributes();
   }

   public void testIsJournalSyncNonTransactional() throws Exception
   {
      boolean journalSyncNonTransactional = randomBoolean();

      expect(configuration.isJournalSyncNonTransactional()).andReturn(
            journalSyncNonTransactional);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(journalSyncNonTransactional, control
            .isJournalSyncNonTransactional());

      verifyMockedAttributes();
   }

   public void testIsJournalSyncTransactional() throws Exception
   {
      boolean journalSyncTransactional = randomBoolean();

      expect(configuration.isJournalSyncTransactional()).andReturn(
            journalSyncTransactional);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(journalSyncTransactional, control
            .isJournalSyncTransactional());

      verifyMockedAttributes();
   }

   public void testIsRequireDestinations() throws Exception
   {
      boolean requireDestinations = randomBoolean();

      expect(configuration.isRequireDestinations()).andReturn(
            requireDestinations);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(requireDestinations, control.isRequireDestinations());

      verifyMockedAttributes();
   }

   public void testIsSSLEnabled() throws Exception
   {
      boolean sslEnabled = randomBoolean();

      expect(configuration.isSSLEnabled()).andReturn(sslEnabled);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(sslEnabled, control.isSSLEnabled());

      verifyMockedAttributes();
   }

   public void testIsSecurityEnabled() throws Exception
   {
      boolean securityEnabled = randomBoolean();

      expect(configuration.isSecurityEnabled()).andReturn(securityEnabled);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(securityEnabled, control.isSecurityEnabled());

      verifyMockedAttributes();
   }

   public void testAddDestination() throws Exception
   {
      String address = randomString();
      boolean added = randomBoolean();

      expect(postOffice.addDestination(new SimpleString(address), false))
            .andReturn(added);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(added, control.addAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_ADDED, listener,
            latch);

      verifyMockedAttributes();

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testRemoveAddress() throws Exception
   {
      String address = randomString();
      boolean removed = randomBoolean();

      expect(postOffice.removeDestination(new SimpleString(address), false))
            .andReturn(removed);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      NotificationListenerWithLatch listener = new NotificationListenerWithLatch(
            latch);
      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(removed, control.removeAddress(address));

      assertGotNotification(
            MessagingServerControl.NotificationType.ADDRESS_REMOVED, listener,
            latch);

      verifyMockedAttributes();

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
      replayMockedAttributes();
      replay(newBinding);

      MessagingServerControl control = createControl();
      control.createQueue(address, name);

      verifyMockedAttributes();
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
      replayMockedAttributes();
      replay(newBinding);

      MessagingServerControl control = createControl();
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
      verifyMockedAttributes();
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
      replayMockedAttributes();
      replay(newBinding);

      MessagingServerControl control = createControl();
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMockedAttributes();
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
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMockedAttributes();
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
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      control.createQueue(address, name, filter, durable);

      verify(newBinding);
      verifyMockedAttributes();
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
      replayMockedAttributes();
      replay(binding, queue);

      MessagingServerControl control = createControl();
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
      verifyMockedAttributes();

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testGetConnectionCount() throws Exception
   {
      int count = randomInt();

      expect(server.getConnectionCount()).andReturn(count);
      replayMockedAttributes();

      MessagingServerControl control = createControl();
      assertEquals(count, control.getConnectionCount());

      verifyMockedAttributes();
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
      messageCounterManager = createMock(MessageCounterManager.class);
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
      messageCounterManager = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private MessagingServerControl createControl() throws Exception
   {
      MessagingServerControl control = new MessagingServerControl(postOffice,
            storageManager, configuration, securityRepository,
            queueSettingsRepository, server, messageCounterManager);
      return control;
   }

   private void replayMockedAttributes()
   {
      replay(postOffice, storageManager, configuration, securityRepository,
            queueSettingsRepository, server, messageCounterManager);
   }

   private void verifyMockedAttributes()
   {
      verify(postOffice, storageManager, configuration, securityRepository,
            queueSettingsRepository, server, messageCounterManager);
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
