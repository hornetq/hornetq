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

package org.jboss.messaging.core.management.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingServerControl extends StandardMBean implements
      MessagingServerControlMBean, NotificationEmitter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final MessagingServerManagement server;
   private final Configuration configuration;

   private final NotificationBroadcasterSupport broadcaster;
   private AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingServerControl(final MessagingServerManagement server,
         final Configuration configuration) throws NotCompliantMBeanException
   {
      super(MessagingServerControlMBean.class);
      this.server = server;
      this.configuration = configuration;
      broadcaster = new NotificationBroadcasterSupport();
   }

   // Public --------------------------------------------------------

   // StandardMBean overrides ---------------------------------------

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), info
            .getAttributes(), info.getConstructors(), MBeanInfoHelper
            .getMBeanOperationsInfo(MessagingServerControlMBean.class),
            getNotificationInfo());
   }

   // MessagingServerControlMBean implementation --------------------

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      return server.getVersion();
   }

   public String getBindingsDirectory()
   {
      return configuration.getBindingsDirectory();
   }

   public List<String> getInterceptorClassNames()
   {
      return configuration.getInterceptorClassNames();
   }

   public String getJournalDirectory()
   {
      return configuration.getJournalDirectory();
   }

   public int getJournalFileSize()
   {
      return configuration.getJournalFileSize();
   }

   public int getJournalMaxAIO()
   {
      return configuration.getJournalMaxAIO();
   }

   public int getJournalMinFiles()
   {
      return configuration.getJournalMinFiles();
   }

   public String getJournalType()
   {
      return configuration.getJournalType().toString();
   }

   public String getKeyStorePath()
   {
      return configuration.getKeyStorePath();
   }

   public String getLocation()
   {
      return configuration.getLocation().toString();
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return configuration.getScheduledThreadPoolMaxSize();
   }

   public long getSecurityInvalidationInterval()
   {
      return configuration.getSecurityInvalidationInterval();
   }

   public String getTrustStorePath()
   {
      return configuration.getTrustStorePath();
   }

   public boolean isClustered()
   {
      return configuration.isClustered();
   }

   public boolean isCreateBindingsDir()
   {
      return configuration.isCreateBindingsDir();
   }

   public boolean isCreateJournalDir()
   {
      return configuration.isCreateJournalDir();
   }

   public boolean isJournalSyncNonTransactional()
   {
      return configuration.isJournalSyncNonTransactional();
   }

   public boolean isJournalSyncTransactional()
   {
      return configuration.isJournalSyncTransactional();
   }

   public boolean isRequireDestinations()
   {
      return configuration.isRequireDestinations();
   }

   public boolean isSSLEnabled()
   {
      return configuration.isSSLEnabled();
   }

   public boolean isSecurityEnabled()
   {
      return configuration.isSecurityEnabled();
   }

   public boolean addAddress(final String address) throws Exception
   {
      sendNotification(NotificationType.ADDRESS_ADDED, address);
      return server.addDestination(new SimpleString(address));
   }

   public void createQueue(final String address, final String name)
         throws Exception
   {
      server.createQueue(new SimpleString(address), new SimpleString(name));
      sendNotification(NotificationType.ADDRESS_ADDED, address);
      sendNotification(NotificationType.QUEUE_CREATED, name);
   }

   public void createQueue(final String address, final String name,
         final String filter, final boolean durable, final boolean temporary)
         throws Exception
   {
      if (temporary && durable)
      {
         throw new IllegalArgumentException(
               "A queue can not be both temporary and durable");
      }

      SimpleString simpleFilter = (filter == null || filter.length() == 0) ? null
            : new SimpleString(filter);
      server.createQueue(new SimpleString(address), new SimpleString(name),
            simpleFilter, durable, temporary);
      sendNotification(NotificationType.ADDRESS_ADDED, address);
      sendNotification(NotificationType.QUEUE_CREATED, name);
   }

   public void destroyQueue(final String name) throws Exception
   {
      server.destroyQueue(new SimpleString(name));
      sendNotification(NotificationType.QUEUE_DESTROYED, name);
   }

   public int getConnectionCount()
   {
      return server.getConnectionCount();
   }

   public boolean removeAddress(final String address) throws Exception
   {
      sendNotification(NotificationType.ADDRESS_REMOVED, address);
      return server.removeDestination(new SimpleString(address));
   }

   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
         final NotificationFilter filter, final Object handback)
         throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener, filter, handback);
   }

   public void removeNotificationListener(final NotificationListener listener)
         throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener);
   }

   public void addNotificationListener(final NotificationListener listener,
         final NotificationFilter filter, final Object handback)
         throws IllegalArgumentException
   {
      broadcaster.addNotificationListener(listener, filter, handback);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      NotificationType[] values = NotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[] { new MBeanNotificationInfo(names,
            this.getClass().getName(), "Notifications emitted by a Core Server") };
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void sendNotification(final NotificationType type,
         final String message)
   {
      Notification notif = new Notification(type.toString(), this, notifSeq
            .incrementAndGet(), message);
      broadcaster.sendNotification(notif);
   }

   // Inner classes -------------------------------------------------

   public static enum NotificationType
   {
      QUEUE_CREATED, QUEUE_DESTROYED, ADDRESS_ADDED, ADDRESS_REMOVED;
   }
}
