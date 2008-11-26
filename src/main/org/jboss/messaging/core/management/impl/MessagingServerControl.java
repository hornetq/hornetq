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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerSessionImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.Base64;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingServerControl extends StandardMBean implements MessagingServerControlMBean, NotificationEmitter
{
   // Constants -----------------------------------------------------

   private static DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

   // Attributes ----------------------------------------------------

   private final PostOffice postOffice;

   private final StorageManager storageManager;

   private final Configuration configuration;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ResourceManager resourceManager;

   private final RemotingService remotingService;

   private final MessagingServer server;

   private final MessageCounterManager messageCounterManager;

   private final NotificationBroadcasterSupport broadcaster;

   private boolean messageCounterEnabled;

   // Static --------------------------------------------------------

   public static String toBase64String(final Xid xid)
   {
      byte[] branchQualifier = xid.getBranchQualifier();
      byte[] globalTransactionId = xid.getGlobalTransactionId();
      int formatId = xid.getFormatId();
      
      byte[] hashBytes = new byte[branchQualifier.length + globalTransactionId.length + 4];
      System.arraycopy(branchQualifier, 0, hashBytes, 0, branchQualifier.length);
      System.arraycopy(globalTransactionId, 0, hashBytes, branchQualifier.length, globalTransactionId.length);
      byte[] intBytes = new byte[4];
      for (int i = 0; i < 4; i++)
      {
         intBytes[i] = (byte)((formatId >> (i * 8)) % 0xFF);
      }
      System.arraycopy(intBytes, 0, hashBytes, branchQualifier.length + globalTransactionId.length, 4);
      return Base64.encodeBytes(hashBytes);
   }
   
   // Constructors --------------------------------------------------

   public MessagingServerControl(final PostOffice postOffice,
                                 final StorageManager storageManager,
                                 final Configuration configuration,
                                 final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                                 final ResourceManager resourceManager,
                                 final RemotingService remotingService,
                                 final MessagingServer messagingServer,
                                 final MessageCounterManager messageCounterManager,
                                 final NotificationBroadcasterSupport broadcaster) throws Exception
   {
      super(MessagingServerControlMBean.class);
      this.postOffice = postOffice;
      this.storageManager = storageManager;
      this.configuration = configuration;
      this.queueSettingsRepository = queueSettingsRepository;
      this.resourceManager = resourceManager;
      this.remotingService = remotingService;
      server = messagingServer;
      this.messageCounterManager = messageCounterManager;
      this.broadcaster = broadcaster;

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

   public Queue getQueue(final String address) throws Exception
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

   public int expireMessages(final Filter filter, final SimpleString simpleAddress) throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         
         //FIXME - what if the refs have been consumed between listing them and expiring them?
         
         for (MessageReference ref : refs)
         {
            queue.expireMessage(ref.getMessage().getMessageID(), storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int sendMessagesToDLQ(final Filter filter, final SimpleString simpleAddress) throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.sendMessageToDeadLetterAddress(ref.getMessage().getMessageID(), storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int changeMessagesPriority(final Filter filter, final byte newPriority, final SimpleString simpleAddress) throws Exception
   {
      Binding binding = postOffice.getBinding(simpleAddress);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.changeMessagePriority(ref.getMessage().getMessageID(),
                                        newPriority,
                                        storageManager,
                                        postOffice,
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
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(MessagingServerControlMBean.class),
                           getNotificationInfo());
   }

   // MessagingServerControlMBean implementation --------------------

   //FIXME
   
   public Map<String, Object> getBackupConnectorConfiguration()
   {
//      TransportConfiguration backupConf = configuration.getBackupConnectorConfiguration();
//      if (backupConf != null)
//      {
//         return backupConf.getParams();
//      }
//      else
//      {
//         return Collections.emptyMap();
//      }
      return Collections.emptyMap();
   }

   //FIXME
   public Map<String, Map<String, Object>> getAcceptorConfigurations()
   {
      Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
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

   //TODO - do we really need this method?
   public void createQueue(final String address, final String name) throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      if (postOffice.getBinding(sAddress) == null)
      {
         postOffice.addBinding(sAddress, sName, null, true, false, true);
      }
   }

   public void createQueue(final String address, final String name, final String filterStr, final boolean durable,
                           final boolean fanout) throws Exception
   {
      SimpleString sAddress = new SimpleString(address);
      SimpleString sName = new SimpleString(name);
      SimpleString sFilter = filterStr == null || filterStr.length() == 0 ? null : new SimpleString(filterStr);
      Filter filter = null;
      if (sFilter != null)
      {
         filter = new FilterImpl(sFilter);
      }
      if (postOffice.getBinding(sAddress) == null)
      {
         postOffice.addBinding(sAddress, sName, filter, durable, false, fanout);
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
         s[i++] = DATE_FORMAT.format(creation) + " base64: " + toBase64String(xid) + " "+ xid.toString();
      }
      return s;
   }
   
   public boolean commitPreparedTransaction(String transactionAsBase64) throws Exception
   {
      List<Xid> xids = resourceManager.getPreparedTransactions();

      for (Xid xid : xids)
      {
         if (toBase64String(xid).equals(transactionAsBase64))
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
         if (toBase64String(xid).equals(transactionAsBase64))
         {
            Transaction transaction = resourceManager.removeTransaction(xid);            
            List<MessageReference> rolledBack = transaction.rollback(queueSettingsRepository);
            
            ServerSessionImpl.moveReferencesBackToHeadOfQueues(rolledBack, postOffice, storageManager, queueSettingsRepository);
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
            connection.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "connections for " + ipAddress + " closed by management"));
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
