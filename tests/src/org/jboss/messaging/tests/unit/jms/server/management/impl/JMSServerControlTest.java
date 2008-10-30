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

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;
import org.jboss.messaging.jms.server.management.impl.JMSServerControl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSServerControlTest extends TestCase
{
   private MBeanServer mbeanServer;
   private ObjectName serverON;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsStarted() throws Exception
   {
      boolean started = randomBoolean();

      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.isStarted()).andReturn(started);

      replay(serverManager);

      JMSServerControl control = new JMSServerControl(serverManager);
      assertEquals(started, control.isStarted());

      verify(serverManager);
   }

   public void testCreateQueueAndReceiveNotification() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      boolean created = true;

      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.createQueue(name, jndiBinding)).andReturn(created);
      replay(serverManager);

      JMSServerControl control = new JMSServerControl(serverManager);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();

      NotificationListener listener = new NotificationListener()
      {
         public void handleNotification(Notification notification,
               Object handback)
         {
            notifRef.set(notification);
            latch.countDown();
         }
      };

      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(created, control.createQueue(name, jndiBinding));

      boolean gotNotification = latch.await(500, MILLISECONDS);
      assertTrue(gotNotification);
      assertNotNull(notifRef.get());
      assertEquals(JMSServerControl.NotificationType.QUEUE_CREATED.toString(),
            notifRef.get().getType());

      verify(serverManager);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testDestroyQueueAndReceiveNotification() throws Exception
   {
      String name = randomString();
      boolean destroyed = true;

      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.destroyQueue(name)).andReturn(destroyed);
      replay(serverManager);

      JMSServerControl control = new JMSServerControl(serverManager);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();

      NotificationListener listener = new NotificationListener()
      {
         public void handleNotification(Notification notification,
               Object handback)
         {
            notifRef.set(notification);
            latch.countDown();
         }
      };

      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(destroyed, control.destroyQueue(name));

      boolean gotNotification = latch.await(500, MILLISECONDS);
      assertTrue(gotNotification);
      assertNotNull(notifRef.get());
      assertEquals(
            JMSServerControl.NotificationType.QUEUE_DESTROYED.toString(),
            notifRef.get().getType());

      verify(serverManager);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testCreateTopicAndReceiveNotification() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      boolean created = true;

      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.createTopic(name, jndiBinding)).andReturn(created);
      replay(serverManager);

      JMSServerControl control = new JMSServerControl(serverManager);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();

      NotificationListener listener = new NotificationListener()
      {
         public void handleNotification(Notification notification,
               Object handback)
         {
            notifRef.set(notification);
            latch.countDown();
         }
      };

      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(created, control.createTopic(name, jndiBinding));

      boolean gotNotification = latch.await(500, MILLISECONDS);
      assertTrue(gotNotification);
      assertNotNull(notifRef.get());
      assertEquals(JMSServerControl.NotificationType.TOPIC_CREATED.toString(),
            notifRef.get().getType());

      verify(serverManager);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

   public void testDestroyTopicAndReceiveNotification() throws Exception
   {
      String name = randomString();
      boolean destroyed = true;

      JMSServerManager serverManager = createMock(JMSServerManager.class);
      expect(serverManager.destroyTopic(name)).andReturn(destroyed);
      replay(serverManager);

      JMSServerControl control = new JMSServerControl(serverManager);
      mbeanServer.registerMBean(control, serverON);

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();

      NotificationListener listener = new NotificationListener()
      {
         public void handleNotification(Notification notification,
               Object handback)
         {
            notifRef.set(notification);
            latch.countDown();
         }
      };

      mbeanServer.addNotificationListener(serverON, listener, null, null);

      assertEquals(destroyed, control.destroyTopic(name));

      boolean gotNotification = latch.await(500, MILLISECONDS);
      assertTrue(gotNotification);
      assertNotNull(notifRef.get());
      assertEquals(
            JMSServerControl.NotificationType.TOPIC_DESTROYED.toString(),
            notifRef.get().getType());

      verify(serverManager);

      mbeanServer.removeNotificationListener(serverON, listener);
   }

//   public void testCreateConnnectionFactoryAndReceiveNotification()
//         throws Exception
//   {
//      String name = randomString();
//      ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
//      Map<String, Object> params = new HashMap<String, Object>();
//      long pingPeriod = randomLong();
//      long callTimeout = randomLong();
//      String clientID = randomString();
//      int dupsOKBatchSize = randomInt();
//      int consumerWindowSize = randomInt();
//      int consumerMaxRate = randomInt();
//      int producerWindowSize = randomInt();
//      int producerMaxRate = randomInt();
//      boolean blockOnAcknowledge = randomBoolean();
//      boolean defaultSendNonPersistentMessagesBlocking = randomBoolean();
//      boolean defaultSendPersistentMessagesBlocking = randomBoolean();
//      boolean created = true;
//      String jndiBinding = randomString();
//    //  List<String> bindings = new ArrayList<String>();
//   //   bindings.add(jndiBinding);
//
//      JMSServerManager serverManager = createMock(JMSServerManager.class);
//      expect(
//            serverManager.createConnectionFactory(name, cf, params,
//                     pingPeriod, callTimeout,
//                     clientID,
//                  dupsOKBatchSize, consumerWindowSize, consumerMaxRate,
//                  producerWindowSize, producerMaxRate, blockOnAcknowledge,
//                  defaultSendNonPersistentMessagesBlocking,
//                  defaultSendPersistentMessagesBlocking, jndiBinding)).andReturn(
//            created);
//      replay(serverManager);
//      
//      JMSServerControl control = new JMSServerControl(serverManager);
//      mbeanServer.registerMBean(control, serverON);
//
//      final CountDownLatch latch = new CountDownLatch(1);
//      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();
//
//      NotificationListener listener = new NotificationListener()
//      {
//         public void handleNotification(Notification notification,
//               Object handback)
//         {
//            notifRef.set(notification);
//            latch.countDown();
//         }
//      };
//
//      mbeanServer.addNotificationListener(serverON, listener, null, null);
//      control.createConnectionFactory(name, cf, params,
//               pingPeriod, callTimeout,
//               clientID,
//            dupsOKBatchSize, consumerWindowSize, consumerMaxRate,
//            producerWindowSize, producerMaxRate, blockOnAcknowledge,
//            defaultSendNonPersistentMessagesBlocking,
//            defaultSendPersistentMessagesBlocking, jndiBinding);
//
//      boolean gotNotification = latch.await(500, MILLISECONDS);
//      assertTrue(gotNotification);
//      assertNotNull(notifRef.get());
//      assertEquals(JMSServerControl.NotificationType.CONNECTION_FACTORY_CREATED
//            .toString(), notifRef.get().getType());
//
//      verify(serverManager);
//
//      mbeanServer.removeNotificationListener(serverON, listener);
//   }
//
//   public void testDestroyConnnectionFactoryAndReceiveNotification()
//         throws Exception
//   {
//      String name = randomString();
//      boolean destroyed = true;
//
//      JMSServerManager serverManager = createMock(JMSServerManager.class);
//      expect(serverManager.destroyConnectionFactory(name)).andReturn(destroyed);
//      replay(serverManager);
//
//      JMSServerControl control = new JMSServerControl(serverManager);
//      mbeanServer.registerMBean(control, serverON);
//
//      final CountDownLatch latch = new CountDownLatch(1);
//      final AtomicReference<Notification> notifRef = new AtomicReference<Notification>();
//
//      NotificationListener listener = new NotificationListener()
//      {
//         public void handleNotification(Notification notification,
//               Object handback)
//         {
//            notifRef.set(notification);
//            latch.countDown();
//         }
//      };
//
//      mbeanServer.addNotificationListener(serverON, listener, null, null);
//      control.destroyConnectionFactory(name);
//
//      boolean gotNotification = latch.await(500, MILLISECONDS);
//      assertTrue(gotNotification);
//      assertNotNull(notifRef.get());
//      assertEquals(
//            JMSServerControl.NotificationType.CONNECTION_FACTORY_DESTROYED
//                  .toString(), notifRef.get().getType());
//
//      verify(serverManager);
//
//      mbeanServer.removeNotificationListener(serverON, listener);
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = ManagementFactory.getPlatformMBeanServer();
      serverON = JMSManagementServiceImpl.getJMSServerObjectName();
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
}
