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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.BridgeControl;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionDetail;
import org.hornetq.core.transaction.impl.CoreTransactionDetail;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.SecurityFormatter;
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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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

   public String getBindingsDirectory()
   {
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      checkStarted();

      clearIO();
      try
      {
         configuration.setFailoverOnServerShutdown(failoverOnServerShutdown);
      }
      finally
      {
         blockOnIO();
      }
   }


   public boolean isFailoverOnServerShutdown()
   {
      checkStarted();

      clearIO();
      try
      {
         return configuration.isFailoverOnServerShutdown();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getJournalMaxIO()
   {
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

      clearIO();
      try
      {
         Object[] queues = server.getManagementService().getResources(QueueControl.class);
         String[] names = new String[queues.length];
         for (int i = 0; i < queues.length; i++)
         {
            QueueControl queue = (QueueControl) queues[i];
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
      checkStarted();

      clearIO();
      try
      {
         Object[] addresses = server.getManagementService().getResources(AddressControl.class);
         String[] names = new String[addresses.length];
         for (int i = 0; i < addresses.length; i++)
         {
            AddressControl address = (AddressControl) addresses[i];
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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

      checkStarted();

      clearIO();
      try
      {
         if (newPeriod < MessageCounterManagerImpl.MIN_SAMPLE_PERIOD)
         {
            throw HornetQMessageBundle.BUNDLE.invalidMessageCounterPeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);
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
      checkStarted();

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
      checkStarted();

      clearIO();
      try
      {
         if (count <= 0)
         {
            throw HornetQMessageBundle.BUNDLE.greaterThanZero(count);
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
      checkStarted();

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
               return (int) (entry1.getValue() - entry2.getValue());
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

   public String listPreparedTransactionDetailsAsJSON() throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         if (xids == null || xids.size() == 0)
         {
            return "";
         }

         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
         {
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2)
            {
               // sort by creation time, oldest first
               return (int) (entry1.getValue() - entry2.getValue());
            }
         });

         JSONArray txDetailListJson = new JSONArray();
         for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime)
         {
            Xid xid = entry.getKey();
            TransactionDetail detail = new CoreTransactionDetail(xid,
                  resourceManager.getTransaction(xid),
                  entry.getValue());

            txDetailListJson.put(detail.toJSON());
         }
         return txDetailListJson.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listPreparedTransactionDetailsAsHTML() throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         if (xids == null || xids.size() == 0)
         {
            return "<h3>*** Prepared Transaction Details ***</h3><p>No entry.</p>";
         }

         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<Map.Entry<Xid, Long>>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>()
         {
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2)
            {
               // sort by creation time, oldest first
               return (int) (entry1.getValue() - entry2.getValue());
            }
         });

         StringBuilder html = new StringBuilder();
         html.append("<h3>*** Prepared Transaction Details ***</h3>");

         for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime)
         {
            Xid xid = entry.getKey();
            TransactionDetail detail = new CoreTransactionDetail(xid,
                  resourceManager.getTransaction(xid),
                  entry.getValue());

            JSONObject txJson = detail.toJSON();

            html.append("<table border=\"1\">");
            html.append("<tr><th>creation_time</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_CREATION_TIME) + "</td>");
            html.append("<th>xid_as_base_64</th>");
            html.append("<td colspan=\"3\">" + txJson.get(TransactionDetail.KEY_XID_AS_BASE64) + "</td></tr>");
            html.append("<tr><th>xid_format_id</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_FORMAT_ID) + "</td>");
            html.append("<th>xid_global_txid</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_GLOBAL_TXID) + "</td>");
            html.append("<th>xid_branch_qual</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_BRANCH_QUAL) + "</td></tr>");

            html.append("<tr><th colspan=\"6\">Message List</th></tr>");
            html.append("<tr><td colspan=\"6\">");
            html.append("<table border=\"1\" cellspacing=\"0\" cellpadding=\"0\">");

            JSONArray msgs = txJson.getJSONArray(TransactionDetail.KEY_TX_RELATED_MESSAGES);
            for (int i = 0; i < msgs.length(); i++)
            {
               JSONObject msgJson = msgs.getJSONObject(i);
               JSONObject props = msgJson.getJSONObject(TransactionDetail.KEY_MSG_PROPERTIES);
               StringBuilder propstr = new StringBuilder();
               @SuppressWarnings("unchecked")
               Iterator<String> propkeys = props.keys();
               while (propkeys.hasNext())
               {
                  String key = propkeys.next();
                  propstr.append(key);
                  propstr.append("=");
                  propstr.append(props.get(key));
                  propstr.append(", ");
               }

               html.append("<th>operation_type</th>");
               html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_OP_TYPE) + "</th>");
               html.append("<th>message_type</th>");
               html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_TYPE) + "</td></tr>");
               html.append("<tr><th>properties</th>");
               html.append("<td colspan=\"3\">" + propstr.toString() + "</td></tr>");
            }
            html.append("</table></td></tr>");
            html.append("</table><br/>");
         }

         return html.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listHeuristicCommittedTransactions()
   {
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
      checkStarted();

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
               connection.fail(HornetQMessageBundle.BUNDLE.connectionsClosedByManagement(ipAddress));
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
      checkStarted();

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
      checkStarted();

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


   /* (non-Javadoc)
   * @see org.hornetq.api.core.management.HornetQServerControl#listProducersInfoAsJSON()
   */
   public String listProducersInfoAsJSON() throws Exception
   {
      JSONArray producers = new JSONArray();


      for (ServerSession session : server.getSessions())
      {
         session.describeProducersInfo(producers);
      }

      return producers.toString();
   }


   public Object[] getConnectors() throws Exception
   {
      checkStarted();

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
      checkStarted();

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

   public void addSecuritySettings(final String addressMatch,
                                   final String sendRoles,
                                   final String consumeRoles,
                                   final String createDurableQueueRoles,
                                   final String deleteDurableQueueRoles,
                                   final String createNonDurableQueueRoles,
                                   final String deleteNonDurableQueueRoles,
                                   final String manageRoles) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         Set<Role> roles = SecurityFormatter.createSecurity(sendRoles,
               consumeRoles,
               createDurableQueueRoles,
               deleteDurableQueueRoles,
               createNonDurableQueueRoles,
               deleteNonDurableQueueRoles,
               manageRoles);

         server.getSecurityRepository().addMatch(addressMatch, roles);

         PersistedRoles persistedRoles = new PersistedRoles(addressMatch,
               sendRoles,
               consumeRoles,
               createDurableQueueRoles,
               deleteDurableQueueRoles,
               createNonDurableQueueRoles,
               deleteNonDurableQueueRoles,
               manageRoles);

         storageManager.storeSecurityRoles(persistedRoles);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void removeSecuritySettings(final String addressMatch) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         server.getSecurityRepository().removeMatch(addressMatch);
         storageManager.deleteSecurityRoles(new SimpleString(addressMatch));
      }
      finally
      {
         blockOnIO();
      }
   }

   public Object[] getRoles(final String addressMatch) throws Exception
   {
      checkStarted();

      checkStarted();

      clearIO();
      try
      {
         Set<Role> roles = server.getSecurityRepository().getMatch(addressMatch);

         Object[] objRoles = new Object[roles.size()];

         int i = 0;
         for (Role role : roles)
         {
            objRoles[i++] = new Object[]{role.getName(),
                  CheckType.SEND.hasRole(role),
                  CheckType.CONSUME.hasRole(role),
                  CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                  CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                  CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                  CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                  CheckType.MANAGE.hasRole(role)};
         }
         return objRoles;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getRolesAsJSON(final String addressMatch) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         JSONArray json = new JSONArray();
         Set<Role> roles = server.getSecurityRepository().getMatch(addressMatch);

         for (Role role : roles)
         {
            json.put(new JSONObject(role));
         }
         return json.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getAddressSettingsAsJSON(final String address) throws Exception
   {
      checkStarted();

      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch(address);
      Map<String, Object> settings = new HashMap<String, Object>();
      if (addressSettings.getDeadLetterAddress() != null)
      {
         settings.put("DLA", addressSettings.getDeadLetterAddress());
      }
      if (addressSettings.getExpiryAddress() != null)
      {
         settings.put("expiryAddress", addressSettings.getExpiryAddress());
      }
      settings.put("expiryDelay", addressSettings.getExpiryDelay());
      settings.put("maxDeliveryAttempts", addressSettings.getMaxDeliveryAttempts());
      settings.put("pageCacheMaxSize", addressSettings.getPageCacheMaxSize());
      settings.put("maxSizeBytes", addressSettings.getMaxSizeBytes());
      settings.put("pageSizeBytes", addressSettings.getPageSizeBytes());
      settings.put("redeliveryDelay", addressSettings.getRedeliveryDelay());
      settings.put("redeliveryMultiplier", addressSettings.getRedeliveryMultiplier());
      settings.put("maxRedeliveryDelay", addressSettings.getMaxRedeliveryDelay());
      settings.put("redistributionDelay", addressSettings.getRedistributionDelay());
      settings.put("lastValueQueue", addressSettings.isLastValueQueue());
      settings.put("sendToDLAOnNoRoute", addressSettings.isSendToDLAOnNoRoute());
      String policy = addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.PAGE ? "PAGE"
            : addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.BLOCK ? "BLOCK"
            : addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.DROP ? "DROP"
            : "FAIL";
      settings.put("addressFullMessagePolicy", policy);

      JSONObject jsonObject = new JSONObject(settings);
      return jsonObject.toString();
   }


   public void addAddressSettings(final String address,
                                  final String DLA,
                                  final String expiryAddress,
                                  final long expiryDelay,
                                  final boolean lastValueQueue,
                                  final int deliveryAttempts,
                                  final long maxSizeBytes,
                                  final int pageSizeBytes,
                                  final int pageMaxCacheSize,
                                  final long redeliveryDelay,
                                  final double redeliveryMultiplier,
                                  final long maxRedeliveryDelay,
                                  final long redistributionDelay,
                                  final boolean sendToDLAOnNoRoute,
                                  final String addressFullMessagePolicy) throws Exception
   {
      checkStarted();

      // JBPAPP-6334 requested this to be pageSizeBytes > maxSizeBytes
      if (pageSizeBytes > maxSizeBytes && maxSizeBytes > 0)
      {
         throw new IllegalStateException("pageSize has to be lower than maxSizeBytes. Invalid argument (" + pageSizeBytes + " < " + maxSizeBytes + ")");
      }

      if (maxSizeBytes < -1)
      {
         throw new IllegalStateException("Invalid argument on maxSizeBytes");
      }

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setDeadLetterAddress(DLA == null ? null : new SimpleString(DLA));
      addressSettings.setExpiryAddress(expiryAddress == null ? null : new SimpleString(expiryAddress));
      addressSettings.setExpiryDelay(expiryDelay);
      addressSettings.setLastValueQueue(lastValueQueue);
      addressSettings.setMaxDeliveryAttempts(deliveryAttempts);
      addressSettings.setPageCacheMaxSize(pageMaxCacheSize);
      addressSettings.setMaxSizeBytes(maxSizeBytes);
      addressSettings.setPageSizeBytes(pageSizeBytes);
      addressSettings.setRedeliveryDelay(redeliveryDelay);
      addressSettings.setRedeliveryMultiplier(redeliveryMultiplier);
      addressSettings.setMaxRedeliveryDelay(maxRedeliveryDelay);
      addressSettings.setRedistributionDelay(redistributionDelay);
      addressSettings.setSendToDLAOnNoRoute(sendToDLAOnNoRoute);
      if (addressFullMessagePolicy == null)
      {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      }
      else if (addressFullMessagePolicy.equalsIgnoreCase("PAGE"))
      {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      }
      else if (addressFullMessagePolicy.equalsIgnoreCase("DROP"))
      {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      }
      else if (addressFullMessagePolicy.equalsIgnoreCase("BLOCK"))
      {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      }
      else if (addressFullMessagePolicy.equalsIgnoreCase("FAIL"))
      {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      }
      server.getAddressSettingsRepository().addMatch(address, addressSettings);

      storageManager.storeAddressSetting(new PersistedAddressSetting(new SimpleString(address), addressSettings));
   }

   public void removeAddressSettings(final String addressMatch) throws Exception
   {
      checkStarted();

      server.getAddressSettingsRepository().removeMatch(addressMatch);
      storageManager.deleteAddressSetting(new SimpleString(addressMatch));
   }

   public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
   {
      checkStarted();

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

   public String[] getDivertNames()
   {
      checkStarted();

      clearIO();
      try
      {
         Object[] diverts = server.getManagementService().getResources(DivertControl.class);
         String[] names = new String[diverts.length];
         for (int i = 0; i < diverts.length; i++)
         {
            DivertControl divert = (DivertControl) diverts[i];
            names[i] = divert.getUniqueName();
         }

         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createDivert(final String name,
                            final String routingName,
                            final String address,
                            final String forwardingAddress,
                            final boolean exclusive,
                            final String filterString,
                            final String transformerClassName) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         DivertConfiguration config = new DivertConfiguration(name,
               routingName,
               address,
               forwardingAddress,
               exclusive,
               filterString,
               transformerClassName);
         server.deployDivert(config);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void destroyDivert(final String name) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         server.destroyDivert(SimpleString.toSimpleString(name));
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getBridgeNames()
   {
      checkStarted();

      clearIO();
      try
      {
         Object[] bridges = server.getManagementService().getResources(BridgeControl.class);
         String[] names = new String[bridges.length];
         for (int i = 0; i < bridges.length; i++)
         {
            BridgeControl bridge = (BridgeControl) bridges[i];
            names[i] = bridge.getName();
         }

         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createBridge(final String name,
                            final String queueName,
                            final String forwardingAddress,
                            final String filterString,
                            final String transformerClassName,
                            final long retryInterval,
                            final double retryIntervalMultiplier,
                            final int reconnectAttempts,
                            final boolean useDuplicateDetection,
                            final int confirmationWindowSize,
                            final long clientFailureCheckPeriod,
                            final String connectorNames,
                            boolean useDiscoveryGroup,
                            final boolean ha,
                            final String user,
                            final String password) throws Exception
   {
      checkStarted();

      clearIO();


      try
      {
         BridgeConfiguration config = null;
         if (useDiscoveryGroup)
         {
            config = new BridgeConfiguration(name,
                  queueName,
                  forwardingAddress,
                  filterString,
                  transformerClassName,
                  HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                  clientFailureCheckPeriod,
                  HornetQClient.DEFAULT_CONNECTION_TTL,
                  retryInterval,
                  HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                  retryIntervalMultiplier,
                  reconnectAttempts,
                  useDuplicateDetection,
                  confirmationWindowSize,
                  connectorNames,
                  ha,
                  user,
                  password);
         }
         else
         {
            List<String> connectors = toList(connectorNames);
            config = new BridgeConfiguration(name,
                  queueName,
                  forwardingAddress,
                  filterString,
                  transformerClassName,
                  HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                  clientFailureCheckPeriod,
                  HornetQClient.DEFAULT_CONNECTION_TTL,
                  retryInterval,
                  HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                  retryIntervalMultiplier,
                  reconnectAttempts,
                  useDuplicateDetection,
                  confirmationWindowSize,
                  connectors,
                  ha,
                  user,
                  password);
         }
         server.deployBridge(config);
      }
      finally
      {
         blockOnIO();
      }
   }


   public void destroyBridge(final String name) throws Exception
   {
      checkStarted();

      clearIO();
      try
      {
         server.destroyBridge(name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void forceFailover() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         server.stop(true);
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
      return new MBeanNotificationInfo[]{new MBeanNotificationInfo(names,
            this.getClass().getName(),
            "Notifications emitted by a Core Server")};
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
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(HornetQServerControl.class);
   }

   private void checkStarted()
   {
      if (!server.isStarted())
      {
         throw new IllegalStateException("HornetQ Server is not started. It can not be managed yet");
      }
   }

   public String[] listTargetAddresses(final String sessionID)
   {
      ServerSession session = server.getSessionByID(sessionID);
      if (session != null)
      {
         return session.getTargetAddresses();
      }
      return new String[0];
   }

   private static List<String> toList(final String commaSeparatedString)
   {
      List<String> list = new ArrayList<String>();
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0)
      {
         return list;
      }
      String[] values = commaSeparatedString.split(",");
      for (int i = 0; i < values.length; i++)
      {
         list.add(values[i].trim());
      }
      return list;
   }

}

