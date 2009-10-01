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

package org.hornetq.tests.integration.management;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;

/**
 * A HornetQServerControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class HornetQServerControlUsingCoreTest extends HornetQServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static String[] toStringArray(Object[] res)
   {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++)
      {
         names[i] = res[i].toString();               
      }
      return names;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // HornetQServerControlTest overrides --------------------------

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
      
      session = null;

      super.tearDown();
   }

   @Override
   protected HornetQServerControl createManagementControl() throws Exception
   {
      return new HornetQServerControl()
      {         
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ResourceNames.CORE_SERVER);
         
         public boolean isSharedStore()
         {
            return (Boolean)proxy.retrieveAttributeValue("sharedStore");
         }
         
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
         
         public void createQueue(String address, String name, boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, durable);
         }
         
         public void deployQueue(String address, String name, String filter, boolean durable) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name, filter, durable);
         }

         public void deployQueue(String address, String name, String filterString) throws Exception
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
            return (String)proxy.retrieveAttributeValue("backupConnectorName");
         }

         public String getBindingsDirectory()
         {
            return (String)proxy.retrieveAttributeValue("bindingsDirectory");
         }

         public Configuration getConfiguration()
         {
            return (Configuration)proxy.retrieveAttributeValue("configuration");
         }

         public int getConnectionCount()
         {
            return (Integer)proxy.retrieveAttributeValue("connectionCount");
         }

         public long getConnectionTTLOverride()
         {
            return (Long)proxy.retrieveAttributeValue("connectionTTLOverride", Long.class);
         }

         public Object[] getConnectors() throws Exception
         {
            return (Object[])proxy.retrieveAttributeValue("connectors");
         }
         
         public String getConnectorsAsJSON() throws Exception
         {
            return (String)proxy.retrieveAttributeValue("connectorsAsJSON");
         }
         
         public String[] getAddressNames()
         {
            return toStringArray((Object[])proxy.retrieveAttributeValue("addressNames"));
         }
         
         public String[] getQueueNames()
         {
            return toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public int getIDCacheSize()
         {
            return (Integer)proxy.retrieveAttributeValue("IDCacheSize");
         }

         public String[] getInterceptorClassNames()
         {
            return toStringArray((Object[])proxy.retrieveAttributeValue("interceptorClassNames"));
         }

         public String getJournalDirectory()
         {
            return (String)proxy.retrieveAttributeValue("journalDirectory");
         }

         public int getJournalFileSize()
         {
            return (Integer)proxy.retrieveAttributeValue("journalFileSize");
         }

         public int getJournalMaxAIO()
         {
            return (Integer)proxy.retrieveAttributeValue("journalMaxAIO");
         }

         public int getJournalMinFiles()
         {
            return (Integer)proxy.retrieveAttributeValue("journalMinFiles");
         }

         public String getJournalType()
         {
            return (String)proxy.retrieveAttributeValue("journalType");
         }

         public String getLargeMessagesDirectory()
         {
            return (String)proxy.retrieveAttributeValue("largeMessagesDirectory");
         }

         public String getManagementAddress()
         {
            return (String)proxy.retrieveAttributeValue("managementAddress");
         }

         public String getManagementNotificationAddress()
         {
            return (String)proxy.retrieveAttributeValue("managementNotificationAddress");
         }

         public long getManagementRequestTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("managementRequestTimeout", Long.class);
         }

         public int getMessageCounterMaxDayCount()
         {
            return (Integer)proxy.retrieveAttributeValue("messageCounterMaxDayCount");
         }

         public long getMessageCounterSamplePeriod()
         {
            return (Long)proxy.retrieveAttributeValue("messageCounterSamplePeriod", Long.class);
         }

         public long getMessageExpiryScanPeriod()
         {
            return (Long)proxy.retrieveAttributeValue("messageExpiryScanPeriod", Long.class);
         }

         public long getMessageExpiryThreadPriority()
         {
            return (Long)proxy.retrieveAttributeValue("messageExpiryThreadPriority", Long.class);
         }

         public String getPagingDirectory()
         {
            return (String)proxy.retrieveAttributeValue("pagingDirectory");
         }

         public long getQueueActivationTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("queueActivationTimeout", Long.class);
         }

         public int getScheduledThreadPoolMaxSize()
         {
            return (Integer)proxy.retrieveAttributeValue("scheduledThreadPoolMaxSize");
         }
         
         public int getThreadPoolMaxSize()
         {
            return (Integer)proxy.retrieveAttributeValue("threadPoolMaxSize");
         }

         public long getSecurityInvalidationInterval()
         {
            return (Long)proxy.retrieveAttributeValue("securityInvalidationInterval", Long.class);
         }

         public long getTransactionTimeout()
         {
            return (Long)proxy.retrieveAttributeValue("transactionTimeout", Long.class);
         }

         public long getTransactionTimeoutScanPeriod()
         {
            return (Long)proxy.retrieveAttributeValue("transactionTimeoutScanPeriod", Long.class);
         }

         public String getVersion()
         {
            return (String)proxy.retrieveAttributeValue("version");
         }

         public boolean isBackup()
         {
            return (Boolean)proxy.retrieveAttributeValue("backup");
         }

         public boolean isClustered()
         {
            return (Boolean)proxy.retrieveAttributeValue("clustered");
         }

         public boolean isCreateBindingsDir()
         {
            return (Boolean)proxy.retrieveAttributeValue("createBindingsDir");
         }

         public boolean isCreateJournalDir()
         {
            return (Boolean)proxy.retrieveAttributeValue("createJournalDir");
         }

         public boolean isJournalSyncNonTransactional()
         {
            return (Boolean)proxy.retrieveAttributeValue("journalSyncNonTransactional");
         }

         public boolean isJournalSyncTransactional()
         {
            return (Boolean)proxy.retrieveAttributeValue("journalSyncTransactional");
         }

         public boolean isMessageCounterEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("messageCounterEnabled");
         }

         public boolean isPersistDeliveryCountBeforeDelivery()
         {
            return (Boolean)proxy.retrieveAttributeValue("persistDeliveryCountBeforeDelivery");
         }

         public boolean isPersistIDCache()
         {
            return (Boolean)proxy.retrieveAttributeValue("persistIDCache");
         }

         public boolean isSecurityEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("securityEnabled");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("started");
         }

         public boolean isWildcardRoutingEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("wildcardRoutingEnabled");
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[])proxy.invokeOperation("listConnectionIDs");
         }

         public String[] listPreparedTransactions() throws Exception
         {
            return (String[])proxy.invokeOperation("listPreparedTransactions");
         }
         
         public String[] listHeuristicCommittedTransactions() throws Exception
         {
            return (String[])proxy.invokeOperation("listHeuristicCommittedTransactions");
         }
         
         public String[] listHeuristicRolledBackTransactions() throws Exception
         {
            return (String[])proxy.invokeOperation("listHeuristicRolledBackTransactions");
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

         public int getAIOBufferSize()
         {
            return (Integer)proxy.retrieveAttributeValue("AIOBufferSize");
         }

         public int getAIOBufferTimeout()
         {
            return (Integer)proxy.retrieveAttributeValue("AIOBufferTimeout");
         }

         public int getJournalCompactMinFiles()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalCompactMinFiles");
         }

         public int getJournalCompactPercentage()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalCompactPercentage");
         }

         public boolean isPersistenceEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("PersistenceEnabled");
         }

      };
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
