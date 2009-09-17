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

package org.hornetq.core.management.jmx.impl;

import javax.management.MBeanInfo;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.management.impl.MBeanInfoHelper;

/**
 * A ReplicationAwareHornetQServerControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareHornetQServerControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         HornetQServerControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final HornetQServerControlImpl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareHornetQServerControlWrapper(final HornetQServerControlImpl localControl,
                                                      final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.CORE_SERVER, HornetQServerControl.class, replicationInvoker);

      this.localControl = localControl;
   }

   // HornetQServerControlMBean implementation ------------------------------

   public String getBackupConnectorName()
   {
      return localControl.getBackupConnectorName();
   }

   public String getBindingsDirectory()
   {
      return localControl.getBindingsDirectory();
   }

   public Configuration getConfiguration()
   {
      return localControl.getConfiguration();
   }

   public int getConnectionCount()
   {
      return localControl.getConnectionCount();
   }

   public String[] getInterceptorClassNames()
   {
      return localControl.getInterceptorClassNames();
   }

   public int getAIOBufferSize()
   {
      return localControl.getAIOBufferSize();
   }

   public int getAIOBufferTimeout()
   {
      return localControl.getAIOBufferTimeout();
   }

   public String getJournalDirectory()
   {
      return localControl.getJournalDirectory();
   }

   public int getJournalFileSize()
   {
      return localControl.getJournalFileSize();
   }

   public int getJournalMaxAIO()
   {
      return localControl.getJournalMaxAIO();
   }

   public int getJournalMinFiles()
   {
      return localControl.getJournalMinFiles();
   }

   public String getJournalType()
   {
      return localControl.getJournalType();
   }

   public int getMessageCounterMaxDayCount()
   {
      return localControl.getMessageCounterMaxDayCount();
   }

   public long getMessageCounterSamplePeriod()
   {
      return localControl.getMessageCounterSamplePeriod();
   }

   public String getPagingDirectory()
   {
      return localControl.getPagingDirectory();
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return localControl.getScheduledThreadPoolMaxSize();
   }

   public int getThreadPoolMaxSize()
   {
      return localControl.getThreadPoolMaxSize();
   }

   public long getSecurityInvalidationInterval()
   {
      return localControl.getSecurityInvalidationInterval();
   }

   public String getVersion()
   {
      return localControl.getVersion();
   }

   public boolean isBackup()
   {
      return localControl.isBackup();
   }

   public boolean isClustered()
   {
      return localControl.isClustered();
   }

   public boolean isCreateBindingsDir()
   {
      return localControl.isCreateBindingsDir();
   }

   public boolean isCreateJournalDir()
   {
      return localControl.isCreateJournalDir();
   }

   public boolean isJournalSyncNonTransactional()
   {
      return localControl.isJournalSyncNonTransactional();
   }

   public boolean isJournalSyncTransactional()
   {
      return localControl.isJournalSyncTransactional();
   }

   public boolean isMessageCounterEnabled()
   {
      return localControl.isMessageCounterEnabled();
   }

   public boolean isSecurityEnabled()
   {
      return localControl.isSecurityEnabled();
   }

   public boolean isStarted()
   {
      return localControl.isStarted();
   }

   public String[] listConnectionIDs()
   {
      return localControl.listConnectionIDs();
   }

   public String[] listPreparedTransactions()
   {
      return localControl.listPreparedTransactions();
   }

   public String[] listRemoteAddresses()
   {
      return localControl.listRemoteAddresses();
   }

   public String[] listRemoteAddresses(String ipAddress)
   {
      return localControl.listRemoteAddresses(ipAddress);
   }

   public String[] listSessions(String connectionID)
   {
      return localControl.listSessions(connectionID);
   }

   public Object[] getConnectors() throws Exception
   {
      return localControl.getConnectors();
   }

   public String getConnectorsAsJSON() throws Exception
   {
      return localControl.getConnectorsAsJSON();
   }

   public String[] getAddressNames()
   {
      return localControl.getAddressNames();
   }

   public String[] getQueueNames()
   {
      return localControl.getQueueNames();
   }

   public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
   {
      replicationAwareInvoke("sendQueueInfoToQueue", queueName, address);
   }

   public boolean addAddress(String address) throws Exception
   {
      return (Boolean)replicationAwareInvoke("addAddress", address);
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      return localControl.closeConnectionsForAddress(ipAddress);
   }

   public boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
   {
      return (Boolean)replicationAwareInvoke("commitPreparedTransaction", transactionAsBase64);
   }

   public void createQueue(final String address, final String name) throws Exception
   {
      replicationAwareInvoke("createQueue", address, name);
   }

   public void createQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
   {
      replicationAwareInvoke("createQueue", address, name, filter, durable);
   }

   public void deployQueue(String address, String name, String filter, boolean durable) throws Exception
   {
      replicationAwareInvoke("deployQueue", address, name, filter, durable);
   }

   public void deployQueue(String address, String name, String filterString) throws Exception
   {
      replicationAwareInvoke("deployQueue", address, name);
   }

   public void destroyQueue(final String name) throws Exception
   {
      replicationAwareInvoke("destroyQueue", name);
   }

   public void disableMessageCounters() throws Exception
   {
      replicationAwareInvoke("disableMessageCounters");
   }

   public void enableMessageCounters() throws Exception
   {
      replicationAwareInvoke("enableMessageCounters");
   }

   public boolean removeAddress(final String address) throws Exception
   {
      return (Boolean)replicationAwareInvoke("removeAddress", address);
   }

   public void resetAllMessageCounterHistories() throws Exception
   {
      replicationAwareInvoke("resetAllMessageCounterHistories");
   }

   public void resetAllMessageCounters() throws Exception
   {
      replicationAwareInvoke("resetAllMessageCounters");
   }

   public boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
   {
      return (Boolean)replicationAwareInvoke("rollbackPreparedTransaction", transactionAsBase64);
   }

   public void setMessageCounterMaxDayCount(final int count) throws Exception
   {
      replicationAwareInvoke("setMessageCounterMaxDayCount", count);
   }

   public void setMessageCounterSamplePeriod(final long newPeriod) throws Exception
   {
      replicationAwareInvoke("setMessageCounterSamplePeriod", newPeriod);
   }

   public long getConnectionTTLOverride()
   {
      return localControl.getConnectionTTLOverride();
   }

   public int getIDCacheSize()
   {
      return localControl.getIDCacheSize();
   }

   public String getLargeMessagesDirectory()
   {
      return localControl.getLargeMessagesDirectory();
   }

   public String getManagementAddress()
   {
      return localControl.getManagementAddress().toString();
   }

   public String getManagementNotificationAddress()
   {
      return localControl.getManagementNotificationAddress().toString();
   }

   public long getManagementRequestTimeout()
   {
      return localControl.getManagementRequestTimeout();
   }

   public long getMessageExpiryScanPeriod()
   {
      return localControl.getMessageExpiryScanPeriod();
   }

   public long getMessageExpiryThreadPriority()
   {
      return localControl.getMessageExpiryThreadPriority();
   }

   public long getQueueActivationTimeout()
   {
      return localControl.getQueueActivationTimeout();
   }

   public long getTransactionTimeout()
   {
      return localControl.getTransactionTimeout();
   }

   public long getTransactionTimeoutScanPeriod()
   {
      return localControl.getTransactionTimeoutScanPeriod();
   }

   public boolean isPersistDeliveryCountBeforeDelivery()
   {
      return localControl.isPersistDeliveryCountBeforeDelivery();
   }

   public boolean isPersistIDCache()
   {
      return localControl.isPersistIDCache();
   }

   public boolean isWildcardRoutingEnabled()
   {
      return localControl.isWildcardRoutingEnabled();
   }

   public int getJournalCompactMinFiles()
   {
      return localControl.getJournalCompactMinFiles();
   }

   public int getJournalCompactPercentage()
   {
      return localControl.getJournalCompactPercentage();
   }

   public boolean isPersistenceEnabled()
   {
      return localControl.isPersistenceEnabled();
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
                           MBeanInfoHelper.getMBeanOperationsInfo(HornetQServerControl.class),
                           localControl.getNotificationInfo());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
