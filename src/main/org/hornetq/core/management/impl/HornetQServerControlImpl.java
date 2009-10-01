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
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;
import javax.transaction.xa.Xid;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.NotificationType;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class HornetQServerControlImpl extends StandardMBean implements HornetQServerControl, NotificationEmitter
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
                                 final NotificationBroadcasterSupport broadcaster) throws Exception
   {
      super(HornetQServerControl.class);
      this.postOffice = postOffice;
      this.configuration = configuration;
      this.resourceManager = resourceManager;
      this.remotingService = remotingService;
      this.server = messagingServer;
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
   
   public boolean isSharedStore()
   {
      return configuration.isSharedStore();
   }

   public String getBackupConnectorName()
   {
      return configuration.getBackupConnectorName();
   }

   public String getBindingsDirectory()
   {
      return configuration.getBindingsDirectory();
   }

   public String[] getInterceptorClassNames()
   {
      return configuration.getInterceptorClassNames().toArray(new String[configuration.getInterceptorClassNames().size()]);
   }

   public int getAIOBufferSize()
   {
      return configuration.getAIOBufferSize();
   }
   
   public int getAIOBufferTimeout()
   {
      return configuration.getAIOBufferTimeout();
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
   
   public int getJournalCompactMinFiles()
   {
      return configuration.getJournalCompactMinFiles();
   }

   public int getJournalCompactPercentage()
   {
      return configuration.getJournalCompactPercentage();
   }

   public boolean isPersistenceEnabled()
   {
      return configuration.isPersistenceEnabled();
   }

   public String getJournalType()
   {
      return configuration.getJournalType().toString();
   }

   public String getPagingDirectory()
   {
      return configuration.getPagingDirectory();
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return configuration.getScheduledThreadPoolMaxSize();
   }
   
   public int getThreadPoolMaxSize()
   {
      return configuration.getThreadPoolMaxSize();
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

   public boolean isSecurityEnabled()
   {
      return configuration.isSecurityEnabled();
   }

   public void deployQueue(final String address, final String name, String filterString) throws Exception
   {
      server.deployQueue(new SimpleString(address), new SimpleString(name), new SimpleString(filterString), true, false);
   }

   public void deployQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception
   {
      SimpleString filter = filterStr == null ? null : new SimpleString(filterStr);

      server.deployQueue(new SimpleString(address), new SimpleString(name), filter, durable, false);
   }

   public void createQueue(final String address, final String name) throws Exception
   {
      server.createQueue(new SimpleString(address), new SimpleString(name), null, true, false);
   }
   
   public void createQueue(final String address, final String name, final boolean durable) throws Exception
   {
      server.createQueue(new SimpleString(address), new SimpleString(name), null, durable, false);
   }

   public void createQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception
   {
      SimpleString filter = null;
      if (filterStr != null && !filterStr.trim().equals(""))
      {
         filter = new SimpleString(filterStr);
      }

      server.createQueue(new SimpleString(address), new SimpleString(name), filter, durable, false);
   }

   public String[] getQueueNames()
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

   public String[] getAddressNames()
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

   public void destroyQueue(final String name) throws Exception
   {
      SimpleString queueName = new SimpleString(name);

      server.destroyQueue(queueName, null);
   }

   public int getConnectionCount()
   {
      return server.getConnectionCount();
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
      return configuration.isMessageCounterEnabled();
   }

   public synchronized long getMessageCounterSamplePeriod()
   {
      return messageCounterManager.getSamplePeriod();
   }

   public synchronized void setMessageCounterSamplePeriod(final long newPeriod)
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
      DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);


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
         s[i++] = dateFormat.format(creation) + " base64: " + XidImpl.toBase64String(xid) + " " + xid.toString();
      }
      return s;
   }
   
   public String[] listHeuristicCommittedTransactions()
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
   
   public String[] listHeuristicRolledBackTransactions()
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

   public synchronized boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
   {
      List<Xid> xids = resourceManager.getPreparedTransactions();

      for (Xid xid : xids)
      {
         if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
         {
            Transaction transaction = resourceManager.removeTransaction(xid);
            transaction.commit();
            long recordID = server.getStorageManager().storeHeuristicCompletion(xid, true);
            resourceManager.putHeuristicCompletion(recordID, xid, true);
            return true;
         }
      }
      return false;
   }

   public synchronized boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
   {
      List<Xid> xids = resourceManager.getPreparedTransactions();

      for (Xid xid : xids)
      {
         if (XidImpl.toBase64String(xid).equals(transactionAsBase64))
         {
            Transaction transaction = resourceManager.removeTransaction(xid);
            transaction.rollback();
            long recordID = server.getStorageManager().storeHeuristicCompletion(xid, false);
            resourceManager.putHeuristicCompletion(recordID, xid, false);
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

   public synchronized boolean closeConnectionsForAddress(final String ipAddress)
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

   public Object[] getConnectors() throws Exception
   {
      Collection<TransportConfiguration> connectorConfigurations = configuration.getConnectorConfigurations().values();
      
      Object[] ret = new Object[connectorConfigurations.size()];
      
      int i = 0;
      for (TransportConfiguration config: connectorConfigurations)
      {
         Object[] tc = new Object[3];
         
         tc[0] = config.getName();
         tc[1] = config.getFactoryClassName();
         tc[2] = config.getParams();
         
         ret[i++] = tc;
      }
      
      return ret;
   }
   
   public String getConnectorsAsJSON() throws Exception
   {
      JSONArray array = new JSONArray();
      
      for (TransportConfiguration config: configuration.getConnectorConfigurations().values())
      {
         array.put(new JSONObject(config));
      }
      
      return array.toString();
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

   public long getManagementRequestTimeout()
   {
      return configuration.getManagementRequestTimeout();
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

}
