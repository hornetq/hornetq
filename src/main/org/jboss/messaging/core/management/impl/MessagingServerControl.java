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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.TabularData;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.management.TransportConfigurationInfo;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.LocalQueueBinding;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingServerControl implements MessagingServerControlMBean, NotificationEmitter
{
   // Constants -----------------------------------------------------

   private static DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

   // Attributes ----------------------------------------------------

   private final PostOffice postOffice;

   private final StorageManager storageManager;

   private final Configuration configuration;

   private final ResourceManager resourceManager;

   private final RemotingService remotingService;

   private final MessagingServer server;

   private final MessageCounterManager messageCounterManager;

   private final NotificationBroadcasterSupport broadcaster;

   private final QueueFactory queueFactory;

   private boolean messageCounterEnabled;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingServerControl(final PostOffice postOffice,
                                 final StorageManager storageManager,
                                 final Configuration configuration,
                                 final ResourceManager resourceManager,
                                 final RemotingService remotingService,
                                 final MessagingServer messagingServer,
                                 final MessageCounterManager messageCounterManager,
                                 final NotificationBroadcasterSupport broadcaster,
                                 final QueueFactory queueFactory) throws Exception
   {
      this.postOffice = postOffice;
      this.storageManager = storageManager;
      this.configuration = configuration;
      this.resourceManager = resourceManager;
      this.remotingService = remotingService;
      server = messagingServer;
      this.messageCounterManager = messageCounterManager;
      this.broadcaster = broadcaster;
      this.queueFactory = queueFactory;

      messageCounterEnabled = configuration.isMessageCounterEnabled();
      if (messageCounterEnabled)
      {
         messageCounterManager.start();
      }
   }

   // Public --------------------------------------------------------

   public void addDestination(final SimpleString simpleAddress) throws Exception
   {
      postOffice.addDestination(simpleAddress, false);
   }

   public void removeDestination(final SimpleString simpleAddress) throws Exception
   {
      postOffice.removeDestination(simpleAddress, false);
   }

   public Queue getQueue(final String name) throws Exception
   {
      SimpleString sName = new SimpleString(name);
      Binding binding = postOffice.getBinding(sName);
      if (binding == null || !binding.isQueueBinding())
      {
         throw new IllegalArgumentException("No queue with name " + sName);
      }

      return (Queue)binding.getBindable();
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   // MessagingServerControlMBean implementation --------------------

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

   public String getBackupConnectorName()
   {
      return configuration.getBackupConnectorName();
   }

   public String getBindingsDirectory()
   {
      return configuration.getBindingsDirectory();
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

   // TODO - do we really need this method?

   public void createQueue(final String address, final String name) throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      if (postOffice.getBinding(sName) == null)
      {
         Queue queue = queueFactory.createQueue(-1, sName, null, true, false);
         Binding binding = new LocalQueueBinding(sAddress, queue);
         storageManager.addQueueBinding(binding);
         postOffice.addBinding(binding);
      }
   }

   public void createQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      SimpleString sFilter = filterStr == null || filterStr.length() == 0 ? null : new SimpleString(filterStr);
      Filter filter = null;
      if (sFilter != null)
      {
         filter = new FilterImpl(sFilter);
      }
      if (postOffice.getBinding(sName) == null)
      {
         Queue queue = queueFactory.createQueue(-1, sName, filter, durable, false);
         Binding binding = new LocalQueueBinding(sAddress, queue);
         if (durable)
         {
            storageManager.addQueueBinding(binding);
         }
         postOffice.addBinding(binding);
      }
   }

   public void destroyQueue(final String name) throws Exception
   {
      SimpleString sName = new SimpleString(name);
      Binding binding = postOffice.getBinding(sName);

      if (binding != null)
      {
         if (binding.isQueueBinding())
         {
            Queue queue = (Queue)binding.getBindable();

            queue.deleteAllReferences();

            postOffice.removeBinding(sName);

            if (queue.isDurable())
            {
               storageManager.deleteQueueBinding(queue.getPersistenceID());
            }
         }
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
      setMessageCounterEnabled(true);
   }

   public void disableMessageCounters()
   {
      setMessageCounterEnabled(false);
   }

   public void resetAllMessageCounters()
   {
      messageCounterManager.resetAllCounters();
   }

   public void resetAllMessageCounterHistories()
   {
      messageCounterManager.resetAllCounterHistories();
   }

   public boolean isMessageCounterEnabled()
   {
      return messageCounterEnabled;
   }

   public synchronized long getMessageCounterSamplePeriod()
   {
      return messageCounterManager.getSamplePeriod();
   }

   public synchronized void setMessageCounterSamplePeriod(final long newPeriod)
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

   public void setMessageCounterMaxDayCount(final int count)
   {
      if (count <= 0)
      {
         throw new IllegalArgumentException("invalid value: count must be greater than 0");
      }
      messageCounterManager.setMaxDayCount(count);
   }

   public String[] listPreparedTransactions()
   {
      Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
      ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
      Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
      {
         public int compare(Entry<Xid, Long> entry1, Entry<Xid, Long> entry2)
         {
            // sort by creation time, oldest first
            return (int)(entry1.getValue() - entry2.getValue());
         }
      });
      String[] s = new String[xidsSortedByCreationTime.size()];
      int i = 0;
      for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime)
      {
         Date creation = new Date(entry.getValue());
         Xid xid = entry.getKey();
         s[i++] = DATE_FORMAT.format(creation) + " base64: " + XidImpl.toBase64String(xid) + " " + xid.toString();
      }
      return s;
   }

   public boolean commitPreparedTransaction(String transactionAsBase64) throws Exception
   {
      List<Xid> xids = resourceManager.getPreparedTransactions();

      for (Xid xid : xids)
      {
         if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
         {
            Transaction transaction = resourceManager.removeTransaction(xid);
            transaction.commit();
            return true;
         }
      }
      return false;
   }

   public boolean rollbackPreparedTransaction(String transactionAsBase64) throws Exception
   {
      List<Xid> xids = resourceManager.getPreparedTransactions();

      for (Xid xid : xids)
      {
         if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
         {
            Transaction transaction = resourceManager.removeTransaction(xid);
            transaction.rollback();
            return true;
         }
      }
      return false;
   }

   public String[] listRemoteAddresses()
   {
      Set<RemotingConnection> connections = remotingService.getConnections();
      String[] remoteAddresses = new String[connections.size()];
      int i = 0;
      for (RemotingConnection connection : connections)
      {
         remoteAddresses[i++] = connection.getRemoteAddress();
      }
      return remoteAddresses;
   }

   public String[] listRemoteAddresses(final String ipAddress)
   {
      Set<RemotingConnection> connections = remotingService.getConnections();
      List<String> remoteConnections = new ArrayList<String>();
      for (RemotingConnection connection : connections)
      {
         String remoteAddress = connection.getRemoteAddress();
         if (remoteAddress.contains(ipAddress))
         {
            remoteConnections.add(connection.getRemoteAddress());
         }
      }
      return (String[])remoteConnections.toArray(new String[remoteConnections.size()]);
   }

   public boolean closeConnectionsForAddress(final String ipAddress)
   {
      boolean closed = false;
      Set<RemotingConnection> connections = remotingService.getConnections();
      for (RemotingConnection connection : connections)
      {
         String remoteAddress = connection.getRemoteAddress();
         if (remoteAddress.contains(ipAddress))
         {
            connection.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "connections for " + ipAddress +
                                                                                      " closed by management"));
            closed = true;
         }
      }

      return closed;
   }

   public String[] listConnectionIDs()
   {
      Set<RemotingConnection> connections = remotingService.getConnections();
      String[] connectionIDs = new String[connections.size()];
      int i = 0;
      for (RemotingConnection connection : connections)
      {
         connectionIDs[i++] = connection.getID().toString();
      }
      return connectionIDs;
   }

   public String[] listSessions(final String connectionID)
   {
      List<ServerSession> sessions = server.getSessions(connectionID);
      String[] sessionIDs = new String[sessions.size()];
      int i = 0;
      for (ServerSession serverSession : sessions)
      {
         sessionIDs[i++] = serverSession.getName();
      }
      return sessionIDs;
   }

   public TabularData getConnectors() throws Exception
   {
      Collection<TransportConfiguration> connectorConfigurations = configuration.getConnectorConfigurations().values();
      return TransportConfigurationInfo.toTabularData(connectorConfigurations);
   }
   
   public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
   {
      postOffice.sendQueueInfoToQueue(new SimpleString(queueName), new SimpleString(address));
   }

   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener, filter, handback);
   }

   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener);
   }

   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException
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
                                                                     this.getClass().getName(),
                                                                     "Notifications emitted by a Core Server") };
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void setMessageCounterEnabled(boolean enable)
   {
      if (isStarted())
      {
         if (messageCounterEnabled && !enable)
         {
            stopMessageCounters();
         }
         else if (!messageCounterEnabled && enable)
         {
            startMessageCounters();
         }
      }
      messageCounterEnabled = enable;
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
}
