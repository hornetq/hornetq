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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
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

   private final PostOffice postOffice;
   private final StorageManager storageManager;
   private final Configuration configuration;
   private final HierarchicalRepository<Set<Role>> securityRepository;
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private final MessagingServer server;
   private final MessageCounterManager messageCounterManager;

   private final NotificationBroadcasterSupport broadcaster;

   private boolean enableMessageCounters;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingServerControl(PostOffice postOffice,
         StorageManager storageManager, Configuration configuration,
         HierarchicalRepository<Set<Role>> securityRepository,
         HierarchicalRepository<QueueSettings> queueSettingsRepository,
         MessagingServer messagingServer, MessageCounterManager messageCounterManager,
         NotificationBroadcasterSupport broadcaster) throws Exception
   {
      super(MessagingServerControlMBean.class);
      this.postOffice = postOffice;
      this.storageManager = storageManager;
      this.configuration = configuration;
      this.securityRepository = securityRepository;
      this.queueSettingsRepository = queueSettingsRepository;
      this.server = messagingServer;
      this.messageCounterManager = messageCounterManager;
      this.broadcaster = broadcaster;
   }

   // Public --------------------------------------------------------

   public void addDestination(SimpleString simpleAddress) throws Exception
   {
      postOffice.addDestination(simpleAddress, false);
   }

   public void removeDestination(SimpleString simpleAddress) throws Exception
   {
      postOffice.removeDestination(simpleAddress, false);
   }

   public Queue getQueue(String address) throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      Binding binding = postOffice.getBinding(sAddress);
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue with name " + sAddress);
      }

      return binding.getQueue();
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public int expireMessages(Filter filter, SimpleString simpleAddress)
         throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.expireMessage(ref.getMessage().getMessageID(),
                  storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int sendMessagesToDLQ(Filter filter, SimpleString simpleAddress)
         throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.sendMessageToDLQ(ref.getMessage().getMessageID(),
                  storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int changeMessagesPriority(Filter filter, byte newPriority,
         SimpleString simpleAddress) throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.changeMessagePriority(ref.getMessage().getMessageID(),
                  newPriority, storageManager, postOffice,
                  queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

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

   public Map<String, Object> getBackupConnectorConfiguration()
   {
      TransportConfiguration backupConf = configuration.getBackupConnectorConfiguration();
      if (backupConf != null)
      {
         return backupConf.getParams();
      } else
      {
         return Collections.emptyMap();
      }
   }
   
   public Map<String, Map<String, Object>> getAcceptorConfigurations()
   {
      Map<String, Map<String, Object>> result = new HashMap<String, Map<String,Object>>();
      Set<TransportConfiguration> acceptorConfs = configuration.getAcceptorConfigurations();
      
      for (TransportConfiguration acceptorConf : acceptorConfs)
      {
         result.put(acceptorConf.getFactoryClassName(), acceptorConf.getParams());
      }
      return result;
   }

   
   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      return server.getVersion().getFullVersion();
   }

   public boolean isBackup()
   {
      return configuration.isBackup();
   }
      
   public String getBindingsDirectory()
   {
      return configuration.getBindingsDirectory();
   }

   public long getCallTimeout()
   {
      return configuration.getCallTimeout();      
   }
   
   public long getConnectionScanPeriod()
   {
      return configuration.getConnectionScanPeriod();      
   }

   public List<String> getInterceptorClassNames()
   {
      return configuration.getInterceptorClassNames();
   }

   public int getJournalBufferReuseSize()
   {
      return configuration.getJournalBufferReuseSize();
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

   public long getPagingMaxGlobalSizeBytes()
   {   
      return configuration.getPagingMaxGlobalSizeBytes();
   }
   
   public int getPacketConfirmationBatchSize()
   {
      return configuration.getPacketConfirmationBatchSize();
   }

   public String getPagingDirectory()
   {      
      return configuration.getPagingDirectory();
   }
   
   public int getScheduledThreadPoolMaxSize()
   {
      return configuration.getScheduledThreadPoolMaxSize();
   }

   public long getSecurityInvalidationInterval()
   {
      return configuration.getSecurityInvalidationInterval();
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

   public boolean isSecurityEnabled()
   {
      return configuration.isSecurityEnabled();
   }

   public boolean addAddress(final String address) throws Exception
   {
      return postOffice.addDestination(new SimpleString(address), false);
   }

   public void createQueue(final String address, final String name)
         throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      if (postOffice.getBinding(sAddress) == null)
      {
         postOffice.addBinding(sAddress, sName, null, true);
      }
   }

   public void createQueue(final String address, final String name,
         final String filterStr, final boolean durable)
         throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      SimpleString sFilter = (filterStr == null || filterStr.length() == 0) ? null
            : new SimpleString(filterStr);
      Filter filter = null;
      if (sFilter != null)
      {
         filter = new FilterImpl(sFilter);
      }
      if (postOffice.getBinding(sAddress) == null)
      {
         postOffice.addBinding(sAddress, sName, filter, durable);
      }
   }

   public void destroyQueue(final String name) throws Exception
   {
      SimpleString sName = new SimpleString(name);
      Binding binding = postOffice.getBinding(sName);

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(storageManager);

         postOffice.removeBinding(queue.getName());
      }
   }

   public int getConnectionCount()
   {
      return server.getConnectionCount();
   }

   public boolean removeAddress(final String address) throws Exception
   {
      return postOffice.removeDestination(new SimpleString(address), false);
   }

   public void enableMessageCounters()
   {
      setEnableMessageCounters(true);
   }

   public void disableMessageCounters()
   {
      setEnableMessageCounters(false);
   }
      
   public void resetAllMessageCounters()
   {
      messageCounterManager.resetAllCounters();
   }

   public void resetAllMessageCounterHistories()
   {
      messageCounterManager.resetAllCounterHistories();
   }

   public boolean isEnableMessageCounters()
   {
      return enableMessageCounters;
   } 
   
   public synchronized long getMessageCounterSamplePeriod()
   {
      return messageCounterManager.getSamplePeriod();
   }

   public synchronized void setMessageCounterSamplePeriod(long newPeriod)
   {
      if (newPeriod < 1000)
      {
         throw new IllegalArgumentException("Cannot set MessageCounterSamplePeriod < 1000 ms");
      }

      if (messageCounterManager != null && newPeriod != messageCounterManager.getSamplePeriod())
      {
         messageCounterManager.reschedule(newPeriod);
      }
   }
   
   public int getMessageCounterMaxDayCount()
   {
      return messageCounterManager.getMaxDayCount();
   }
   
   public void setMessageCounterMaxDayCount(int count)
   {
      if (count <= 0)
      {
         throw new IllegalArgumentException("invalid value: count must be greater than 0");
      }
      messageCounterManager.setMaxDayCount(count);
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

   private synchronized void setEnableMessageCounters(boolean enable) 
   {
      if (isStarted())
      {
         if (enableMessageCounters && !enable)
         {
            stopMessageCounters();
         }
         else if (!enableMessageCounters && enable)
         {
            startMessageCounters();
         }        
      }
      enableMessageCounters = enable;
   }

   private void startMessageCounters()
   {
      messageCounterManager.start();
   }
   
   private void stopMessageCounters()
   {
      messageCounterManager.stop();
      
      messageCounterManager.resetAllCounters();

      messageCounterManager.resetAllCounterHistories();
   }

   // Inner classes -------------------------------------------------

   public static enum NotificationType
   {
      QUEUE_CREATED, QUEUE_DESTROYED, ADDRESS_ADDED, ADDRESS_REMOVED;
   }
}
