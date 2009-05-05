/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.management;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;

/**
 * A MessagingServerControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class MessagingServerControlUsingCoreTest extends MessagingServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // MessagingServerControlTest overrides --------------------------

   private ClientSession session;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();

   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      super.tearDown();
   }

   @Override
   protected MessagingServerControlMBean createManagementControl() throws Exception
   {

      return new MessagingServerControlMBean()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ResourceNames.CORE_SERVER);
         
         public boolean closeConnectionsForAddress(String ipAddress) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean commitPreparedTransaction(String transactionAsBase64) throws Exception
         {
            return (Boolean)proxy.invokeOperation("commitPreparedTransaction", transactionAsBase64);
         }

         public void createQueue(String address, String name) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name);
         }

         public void createQueue(String address, String name, String filter, boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, filter, durable);
         }
         
         public void deployQueue(String address, String name, String filter, boolean durable) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name, filter, durable);
         }

         public void deployQueue(String address, String name) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name);
         }

         public void destroyQueue(String name) throws Exception
         {
            proxy.invokeOperation("destroyQueue", name);
         }

         public void disableMessageCounters() throws Exception
         {
            proxy.invokeOperation("disableMessageCounters");
         }

         public void enableMessageCounters() throws Exception
         {
            proxy.invokeOperation("enableMessageCounters");
         }

         public String getBackupConnectorName()
         {
            return (String)proxy.retrieveAttributeValue("BackupConnectorName");
         }

         public String getBindingsDirectory()
         {
            return (String)proxy.retrieveAttributeValue("BindingsDirectory");
         }

         public Configuration getConfiguration()
         {
            return (Configuration)proxy.retrieveAttributeValue("Configuration");
         }

         public int getConnectionCount()
         {
            return (Integer)proxy.retrieveAttributeValue("ConnectionCount");
         }

         public long getConnectionScanPeriod()
         {
            return (Long)proxy.retrieveAttributeValue("ConnectionScanPeriod", Long.class);
         }

         public long getConnectionTTLOverride()
         {
            return (Long)proxy.retrieveAttributeValue("ConnectionTTLOverride", Long.class);
         }

         public Object[] getConnectors() throws Exception
         {
            return (Object[])proxy.retrieveAttributeValue("Connectors");
         }

         public int getIDCacheSize()
         {
            return (Integer)proxy.retrieveAttributeValue("IDCacheSize");
         }

         public String[] getInterceptorClassNames()
         {
            Object[] res = (Object[])proxy.retrieveAttributeValue("InterceptorClassNames");
            String[] names = new String[res.length];
            for (int i = 0; i < res.length; i++)
            {
               names[i] = res[i].toString();               
            }
            return names;
         }

         public int getJournalBufferReuseSize()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalBufferReuseSize");
         }

         public String getJournalDirectory()
         {
            return (String)proxy.retrieveAttributeValue("JournalDirectory");
         }

         public int getJournalFileSize()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalFileSize");
         }

         public int getJournalMaxAIO()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalMaxAIO");
         }

         public int getJournalMinFiles()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalMinFiles");
         }

         public String getJournalType()
         {
            return (String)proxy.retrieveAttributeValue("JournalType");
         }

         public String getLargeMessagesDirectory()
         {
            return (String)proxy.retrieveAttributeValue("LargeMessagesDirectory");
         }

         public String getManagementAddress()
         {
            return (String)proxy.retrieveAttributeValue("ManagementAddress");
         }

         public String getManagementNotificationAddress()
         {
            return (String)proxy.retrieveAttributeValue("ManagementNotificationAddress");
         }

         public long getManagementRequestTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("ManagementRequestTimeout", Long.class);
         }

         public int getMessageCounterMaxDayCount()
         {
            return (Integer)proxy.retrieveAttributeValue("MessageCounterMaxDayCount");
         }

         public long getMessageCounterSamplePeriod()
         {
            return (Long)proxy.retrieveAttributeValue("MessageCounterSamplePeriod", Long.class);
         }

         public long getMessageExpiryScanPeriod()
         {
            return (Long)proxy.retrieveAttributeValue("MessageExpiryScanPeriod", Long.class);
         }

         public long getMessageExpiryThreadPriority()
         {
            return (Long)proxy.retrieveAttributeValue("MessageExpiryThreadPriority", Long.class);
         }

         public String getPagingDirectory()
         {
            return (String)proxy.retrieveAttributeValue("PagingDirectory");
         }

         public int getPagingGlobalWatermarkSize()
         {
            return (Integer)proxy.retrieveAttributeValue("PagingGlobalWatermarkSize");
         }

         public long getPagingMaxGlobalSizeBytes()
         {
            return (Long)proxy.retrieveAttributeValue("PagingMaxGlobalSizeBytes", Long.class);
         }

         public long getQueueActivationTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("QueueActivationTimeout", Long.class);
         }

         public int getScheduledThreadPoolMaxSize()
         {
            return (Integer)proxy.retrieveAttributeValue("ScheduledThreadPoolMaxSize");
         }
         
         public int getThreadPoolMaxSize()
         {
            return (Integer)proxy.retrieveAttributeValue("ThreadPoolMaxSize");
         }

         public long getSecurityInvalidationInterval()
         {
            return (Long)proxy.retrieveAttributeValue("SecurityInvalidationInterval", Long.class);
         }

         public long getTransactionTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("TransactionTimeout", Long.class);
         }

         public long getTransactionTimeoutScanPeriod()
         {
            return (Long)proxy.retrieveAttributeValue("TransactionTimeoutScanPeriod", Long.class);
         }

         public String getVersion()
         {
            return (String)proxy.retrieveAttributeValue("Version");
         }

         public boolean isAllowRouteWhenNoBindings()
         {
            return (Boolean)proxy.retrieveAttributeValue("AllowRouteWhenNoBindings");
         }

         public boolean isBackup()
         {
            return (Boolean)proxy.retrieveAttributeValue("Backup");
         }

         public boolean isClustered()
         {
            return (Boolean)proxy.retrieveAttributeValue("Clustered");
         }

         public boolean isCreateBindingsDir()
         {
            return (Boolean)proxy.retrieveAttributeValue("CreateBindingsDir");
         }

         public boolean isCreateJournalDir()
         {
            return (Boolean)proxy.retrieveAttributeValue("CreateJournalDir");
         }

         public boolean isJournalSyncNonTransactional()
         {
            return (Boolean)proxy.retrieveAttributeValue("JournalSyncNonTransactional");
         }

         public boolean isJournalSyncTransactional()
         {
            return (Boolean)proxy.retrieveAttributeValue("JournalSyncTransactional");
         }

         public boolean isMessageCounterEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("MessageCounterEnabled");
         }

         public boolean isPersistDeliveryCountBeforeDelivery()
         {
            return (Boolean)proxy.retrieveAttributeValue("PersistDeliveryCountBeforeDelivery");
         }

         public boolean isPersistIDCache()
         {
            return (Boolean)proxy.retrieveAttributeValue("PersistIDCache");
         }

         public boolean isSecurityEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("SecurityEnabled");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("Started");
         }

         public boolean isWildcardRoutingEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("WildcardRoutingEnabled");
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[])proxy.invokeOperation("listConnectionIDs");
         }

         public String[] listPreparedTransactions() throws Exception
         {
            return (String[])proxy.invokeOperation("listPreparedTransactions");
         }

         public String[] listRemoteAddresses() throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses");
         }

         public String[] listRemoteAddresses(String ipAddress) throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(String connectionID) throws Exception
         {
            return (String[])proxy.invokeOperation("listSessions", connectionID);
         }

         public void resetAllMessageCounterHistories() throws Exception
         {
            proxy.invokeOperation("resetAllMessageCounterHistories");
         }

         public void resetAllMessageCounters() throws Exception
         {
            proxy.invokeOperation("resetAllMessageCounters");
         }

         public boolean rollbackPreparedTransaction(String transactionAsBase64) throws Exception
         {
            return (Boolean)proxy.invokeOperation("rollbackPreparedTransaction", transactionAsBase64);
         }

         public void sendQueueInfoToQueue(String queueName, String address) throws Exception
         {
            proxy.invokeOperation("sendQueueInfoToQueue", queueName, address);
         }

         public void setMessageCounterMaxDayCount(int count) throws Exception
         {
            proxy.invokeOperation("setMessageCounterMaxDayCount", count);
         }

         public void setMessageCounterSamplePeriod(long newPeriod) throws Exception
         {
            proxy.invokeOperation("setMessageCounterSamplePeriod", newPeriod);
         }

      };
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
