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

package org.hornetq.core.management.impl;

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
import javax.management.MBeanOperationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class HornetQServerControlImpl extends AbstractControl implements HornetQServerControl, NotificationEmitter
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQServerControlImpl.class);

   // Attributes ----------------------------------------------------

   private final PostOffice postOffice;

   private final Configuration configuration;

   private final ResourceManager resourceManager;

   private final RemotingService remotingService;

   private final HornetQServer server;

   private final MessageCounterManager messageCounterManager;

   private final NotificationBroadcasterSupport broadcaster;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQServerControlImpl(final PostOffice postOffice,
                                   final Configuration configuration,
                                   final ResourceManager resourceManager,
                                   final RemotingService remotingService,
                                   final HornetQServer messagingServer,
                                   final MessageCounterManager messageCounterManager,
                                   final StorageManager storageManager,
                                   final NotificationBroadcasterSupport broadcaster) throws Exception
   {
      super(HornetQServerControl.class, storageManager);
      this.postOffice = postOffice;
      this.configuration = configuration;
      this.resourceManager = resourceManager;
      this.remotingService = remotingService;
      server = messagingServer;
      this.messageCounterManager = messageCounterManager;
      this.broadcaster = broadcaster;
   }

   // Public --------------------------------------------------------

   public Configuration getConfiguration()
   {
      return configuration;
   }

   // HornetQServerControlMBean implementation --------------------

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return server.isStarted();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getVersion()
   {
      clearIO();
      try
      {
         return server.getVersion().getFullVersion();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isBackup()
   {
      clearIO();
      try
      {
         return configuration.isBackup();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isSharedStore()
   {
      clearIO();
      try
      {
         return configuration.isSharedStore();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getBackupConnectorName()
   {
      clearIO();
      try
      {
         return configuration.getBackupConnectorName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getBindingsDirectory()
   {
      clearIO();
      try
      {
         return configuration.getBindingsDirectory();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getInterceptorClassNames()
   {
      clearIO();
      try
      {
         return configuration.getInterceptorClassNames().toArray(new String[configuration.getInterceptorClassNames()
                                                                                         .size()]);
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalBufferSize()
   {
      clearIO();
      try
      {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalBufferSize_AIO()
                                                                     : configuration.getJournalBufferSize_NIO();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalBufferTimeout()
   {
      clearIO();
      try
      {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalBufferTimeout_AIO()
                                                                     : configuration.getJournalBufferTimeout_NIO();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalMaxIO()
   {
      clearIO();
      try
      {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalMaxIO_AIO()
                                                                     : configuration.getJournalMaxIO_NIO();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getJournalDirectory()
   {
      clearIO();
      try
      {
         return configuration.getJournalDirectory();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalFileSize()
   {
      clearIO();
      try
      {
         return configuration.getJournalFileSize();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalMinFiles()
   {
      clearIO();
      try
      {
         return configuration.getJournalMinFiles();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalCompactMinFiles()
   {
      clearIO();
      try
      {
         return configuration.getJournalCompactMinFiles();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalCompactPercentage()
   {
      clearIO();
      try
      {
         return configuration.getJournalCompactPercentage();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isPersistenceEnabled()
   {
      clearIO();
      try
      {
         return configuration.isPersistenceEnabled();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getJournalType()
   {
      clearIO();
      try
      {
         return configuration.getJournalType().toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getPagingDirectory()
   {
      clearIO();
      try
      {
         return configuration.getPagingDirectory();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getScheduledThreadPoolMaxSize()
   {
      clearIO();
      try
      {
         return configuration.getScheduledThreadPoolMaxSize();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getThreadPoolMaxSize()
   {
      clearIO();
      try
      {
         return configuration.getThreadPoolMaxSize();
      }
      finally
      {
         blockOnIO();
      }
   }

   public long getSecurityInvalidationInterval()
   {
      clearIO();
      try
      {
         return configuration.getSecurityInvalidationInterval();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isClustered()
   {
      clearIO();
      try
      {
         return configuration.isClustered();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isCreateBindingsDir()
   {
      clearIO();
      try
      {
         return configuration.isCreateBindingsDir();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isCreateJournalDir()
   {
      clearIO();
      try
      {
         return configuration.isCreateJournalDir();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isJournalSyncNonTransactional()
   {
      clearIO();
      try
      {
         return configuration.isJournalSyncNonTransactional();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isJournalSyncTransactional()
   {
      clearIO();
      try
      {
         return configuration.isJournalSyncTransactional();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isSecurityEnabled()
   {
      clearIO();
      try
      {
         return configuration.isSecurityEnabled();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isAsyncConnectionExecutionEnabled()
   {
      clearIO();
      try
      {
         return configuration.isAsyncConnectionExecutionEnabled();
      }
      finally
      {
         blockOnIO();
      }
   }
   public void deployQueue(final String address, final String name, final String filterString) throws Exception
   {
      clearIO();
      try
      {
         server.deployQueue(new SimpleString(address),
                            new SimpleString(name),
                            new SimpleString(filterString),
                            true,
                            false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void deployQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception
   {
      SimpleString filter = filterStr == null ? null : new SimpleString(filterStr);
      clearIO();
      try
      {

         server.deployQueue(new SimpleString(address), new SimpleString(name), filter, durable, false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createQueue(final String address, final String name) throws Exception
   {
      clearIO();
      try
      {
         server.createQueue(new SimpleString(address), new SimpleString(name), null, true, false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createQueue(final String address, final String name, final boolean durable) throws Exception
   {
      clearIO();
      try
      {
         server.createQueue(new SimpleString(address), new SimpleString(name), null, durable, false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception
   {
      clearIO();
      try
      {
         SimpleString filter = null;
         if (filterStr != null && !filterStr.trim().equals(""))
         {
            filter = new SimpleString(filterStr);
         }

         server.createQueue(new SimpleString(address), new SimpleString(name), filter, durable, false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getQueueNames()
   {
      clearIO();
      try
      {
         Object[] queues = server.getManagementService().getResources(QueueControl.class);
         String[] names = new String[queues.length];
         for (int i = 0; i < queues.length; i++)
         {
            QueueControl queue = (QueueControl)queues[i];
            names[i] = queue.getName();
         }

         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getAddressNames()
   {
      clearIO();
      try
      {
         Object[] addresses = server.getManagementService().getResources(AddressControl.class);
         String[] names = new String[addresses.length];
         for (int i = 0; i < addresses.length; i++)
         {
            AddressControl address = (AddressControl)addresses[i];
            names[i] = address.getAddress();
         }

         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public void destroyQueue(final String name) throws Exception
   {
      clearIO();
      try
      {
         SimpleString queueName = new SimpleString(name);

         server.destroyQueue(queueName, null);
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getConnectionCount()
   {
      clearIO();
      try
      {
         return server.getConnectionCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void enableMessageCounters()
   {
      clearIO();
      try
      {
         setMessageCounterEnabled(true);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void disableMessageCounters()
   {
      clearIO();
      try
      {
         setMessageCounterEnabled(false);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void resetAllMessageCounters()
   {
      clearIO();
      try
      {
         messageCounterManager.resetAllCounters();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void resetAllMessageCounterHistories()
   {
      clearIO();
      try
      {
         messageCounterManager.resetAllCounterHistories();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isMessageCounterEnabled()
   {
      clearIO();
      try
      {
         return configuration.isMessageCounterEnabled();
      }
      finally
      {
         blockOnIO();
      }
   }

   public synchronized long getMessageCounterSamplePeriod()
   {
      clearIO();
      try
      {
         return messageCounterManager.getSamplePeriod();
      }
      finally
      {
         blockOnIO();
      }
   }

   public synchronized void setMessageCounterSamplePeriod(final long newPeriod)
   {
      clearIO();
      try
      {
         if (newPeriod < MessageCounterManagerImpl.MIN_SAMPLE_PERIOD)
         {
            throw new IllegalArgumentException("Cannot set MessageCounterSamplePeriod < " + MessageCounterManagerImpl.MIN_SAMPLE_PERIOD +
                                               " ms");
         }

         if (messageCounterManager != null && newPeriod != messageCounterManager.getSamplePeriod())
         {
            messageCounterManager.reschedule(newPeriod);
         }
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getMessageCounterMaxDayCount()
   {
      clearIO();
      try
      {
         return messageCounterManager.getMaxDayCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void setMessageCounterMaxDayCount(final int count)
   {
      clearIO();
      try
      {
         if (count <= 0)
         {
            throw new IllegalArgumentException("invalid value: count must be greater than 0");
         }
         messageCounterManager.setMaxDayCount(count);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listPreparedTransactions()
   {
      clearIO();
      try
      {
         DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
         {
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2)
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
            s[i++] = dateFormat.format(creation) + " base64: " + XidImpl.toBase64String(xid) + " " + xid.toString();
         }
         return s;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listHeuristicCommittedTransactions()
   {
      clearIO();
      try
      {
         List<Xid> xids = resourceManager.getHeuristicCommittedTransactions();
         String[] s = new String[xids.size()];
         int i = 0;
         for (Xid xid : xids)
         {
            s[i++] = XidImpl.toBase64String(xid);
         }
         return s;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listHeuristicRolledBackTransactions()
   {
      clearIO();
      try
      {
         List<Xid> xids = resourceManager.getHeuristicRolledbackTransactions();
         String[] s = new String[xids.size()];
         int i = 0;
         for (Xid xid : xids)
         {
            s[i++] = XidImpl.toBase64String(xid);
         }
         return s;
      }
      finally
      {
         blockOnIO();
      }
   }

   public synchronized boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
   {
      clearIO();
      try
      {
         List<Xid> xids = resourceManager.getPreparedTransactions();

         for (Xid xid : xids)
         {
            if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
            {
               Transaction transaction = resourceManager.removeTransaction(xid);
               transaction.commit(false);
               long recordID = server.getStorageManager().storeHeuristicCompletion(xid, true);
               storageManager.waitOnOperations();
               resourceManager.putHeuristicCompletion(recordID, xid, true);
               return true;
            }
         }
         return false;
      }
      finally
      {
         blockOnIO();
      }
   }

   public synchronized boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
   {

      clearIO();
      try
      {

         List<Xid> xids = resourceManager.getPreparedTransactions();

         for (Xid xid : xids)
         {
            if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
            {
               Transaction transaction = resourceManager.removeTransaction(xid);
               transaction.rollback();
               long recordID = server.getStorageManager().storeHeuristicCompletion(xid, false);
               server.getStorageManager().waitOnOperations();
               resourceManager.putHeuristicCompletion(recordID, xid, false);
               return true;
            }
         }
         return false;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listRemoteAddresses()
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }

   }

   public String[] listRemoteAddresses(final String ipAddress)
   {
      clearIO();
      try
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
         return remoteConnections.toArray(new String[remoteConnections.size()]);
      }
      finally
      {
         blockOnIO();
      }

   }

   public synchronized boolean closeConnectionsForAddress(final String ipAddress)
   {
      clearIO();
      try
      {
         boolean closed = false;
         Set<RemotingConnection> connections = remotingService.getConnections();
         for (RemotingConnection connection : connections)
         {
            String remoteAddress = connection.getRemoteAddress();
            if (remoteAddress.contains(ipAddress))
            {
               remotingService.removeConnection(connection.getID());
               connection.fail(new HornetQException(HornetQException.INTERNAL_ERROR, "connections for " + ipAddress +
                                                                                     " closed by management"));
               closed = true;
            }
         }

         return closed;
      }
      finally
      {
         blockOnIO();
      }

   }

   public String[] listConnectionIDs()
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public String[] listSessions(final String connectionID)
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public Object[] getConnectors() throws Exception
   {
      clearIO();
      try
      {
         Collection<TransportConfiguration> connectorConfigurations = configuration.getConnectorConfigurations()
                                                                                   .values();

         Object[] ret = new Object[connectorConfigurations.size()];

         int i = 0;
         for (TransportConfiguration config : connectorConfigurations)
         {
            Object[] tc = new Object[3];

            tc[0] = config.getName();
            tc[1] = config.getFactoryClassName();
            tc[2] = config.getParams();

            ret[i++] = tc;
         }

         return ret;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getConnectorsAsJSON() throws Exception
   {
      clearIO();
      try
      {
         JSONArray array = new JSONArray();

         for (TransportConfiguration config : configuration.getConnectorConfigurations().values())
         {
            array.put(new JSONObject(config));
         }

         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
   {
      clearIO();
      try
      {
         postOffice.sendQueueInfoToQueue(new SimpleString(queueName), new SimpleString(address));
      }
      finally
      {
         blockOnIO();
      }
   }

   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException
   {
      clearIO();
      try
      {
         broadcaster.removeNotificationListener(listener, filter, handback);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException
   {
      clearIO();
      try
      {
         broadcaster.removeNotificationListener(listener);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException
   {
      clearIO();
      try
      {
         broadcaster.addNotificationListener(listener, filter, handback);
      }
      finally
      {
         blockOnIO();
      }
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

   private synchronized void setMessageCounterEnabled(final boolean enable)
   {
      if (isStarted())
      {
         if (configuration.isMessageCounterEnabled() && !enable)
         {
            stopMessageCounters();
         }
         else if (!configuration.isMessageCounterEnabled() && enable)
         {
            startMessageCounters();
         }
      }
      configuration.setMessageCounterEnabled(enable);
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

   public long getConnectionTTLOverride()
   {
      return configuration.getConnectionTTLOverride();
   }

   public int getIDCacheSize()
   {
      return configuration.getIDCacheSize();
   }

   public String getLargeMessagesDirectory()
   {
      return configuration.getLargeMessagesDirectory();
   }

   public String getManagementAddress()
   {
      return configuration.getManagementAddress().toString();
   }

   public String getManagementNotificationAddress()
   {
      return configuration.getManagementNotificationAddress().toString();
   }

   public long getMessageExpiryScanPeriod()
   {
      return configuration.getMessageExpiryScanPeriod();
   }

   public long getMessageExpiryThreadPriority()
   {
      return configuration.getMessageExpiryThreadPriority();
   }

   public long getTransactionTimeout()
   {
      return configuration.getTransactionTimeout();
   }

   public long getTransactionTimeoutScanPeriod()
   {
      return configuration.getTransactionTimeoutScanPeriod();
   }

   public boolean isPersistDeliveryCountBeforeDelivery()
   {
      return configuration.isPersistDeliveryCountBeforeDelivery();
   }

   public boolean isPersistIDCache()
   {
      return configuration.isPersistIDCache();
   }

   public boolean isWildcardRoutingEnabled()
   {
      return configuration.isWildcardRoutingEnabled();
   }

   @Override
   MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(HornetQServerControl.class);
   }

}
