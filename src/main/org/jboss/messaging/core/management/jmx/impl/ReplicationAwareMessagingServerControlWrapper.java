/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management.jmx.impl;

import java.util.List;
import java.util.Map;

import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.impl.MessagingServerControl;

/**
 * A ReplicationAwareMessagingServerControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareMessagingServerControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         MessagingServerControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final MessagingServerControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareMessagingServerControlWrapper(final ObjectName objectName,
                                                        final MessagingServerControl localControl) throws Exception
   {
      super(objectName, MessagingServerControlMBean.class);

      this.localControl = localControl;
   }

   // MessagingServerControlMBean implementation ------------------------------

   
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

   public long getConnectionScanPeriod()
   {
      return localControl.getConnectionScanPeriod();
   }

   public List<String> getInterceptorClassNames()
   {
      return localControl.getInterceptorClassNames();
   }

   public int getJournalBufferReuseSize()
   {
      return localControl.getJournalBufferReuseSize();
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

   public long getPagingMaxGlobalSizeBytes()
   {
      return localControl.getPagingMaxGlobalSizeBytes();
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return localControl.getScheduledThreadPoolMaxSize();
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

   public boolean isRequireDestinations()
   {
      return localControl.isRequireDestinations();
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
   
   public TabularData getConnectors() throws Exception
   {
      return localControl.getConnectors();
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

   public void createQueue(final String address, final String name, final String filter, final boolean durable, final boolean fanout) throws Exception
   {
      replicationAwareInvoke("createQueue", address, name, filter, durable, fanout);
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
                           localControl.getNotificationInfo());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
