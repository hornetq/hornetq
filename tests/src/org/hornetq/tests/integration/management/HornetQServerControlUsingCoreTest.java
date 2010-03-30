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

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.Parameter;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.json.JSONObject;

import java.util.Set;

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

   private static String[] toStringArray(final Object[] res)
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

      ClientSessionFactory sf = HornetQClient.createClientSessionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
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
   
   protected void restartServer() throws Exception
   {
      session.close();
      
      super.restartServer();
      
      ClientSessionFactory sf = HornetQClient.createClientSessionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();

   }

   @Override
   protected HornetQServerControl createManagementControl() throws Exception
   {
      return new HornetQServerControl()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_SERVER);

         public boolean isSharedStore()
         {
            return (Boolean)proxy.retrieveAttributeValue("sharedStore");
         }

         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
         {
            return (Boolean)proxy.invokeOperation("commitPreparedTransaction", transactionAsBase64);
         }

         public void createQueue(final String address, final String name) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name);
         }

         public void createQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, filter, durable);
         }

         public void createQueue(final String address, final String name, final boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, durable);
         }

         public void deployQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name, filter, durable);
         }

         public void deployQueue(final String address, final String name, final String filterString) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name);
         }

         public void destroyQueue(final String name) throws Exception
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
            return HornetQServerControlUsingCoreTest.toStringArray((Object[])proxy.retrieveAttributeValue("addressNames"));
         }

         public String[] getQueueNames()
         {
            return HornetQServerControlUsingCoreTest.toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public int getIDCacheSize()
         {
            return (Integer)proxy.retrieveAttributeValue("IDCacheSize");
         }

         public String[] getInterceptorClassNames()
         {
            return HornetQServerControlUsingCoreTest.toStringArray((Object[])proxy.retrieveAttributeValue("interceptorClassNames"));
         }

         public String getJournalDirectory()
         {
            return (String)proxy.retrieveAttributeValue("journalDirectory");
         }

         public int getJournalFileSize()
         {
            return (Integer)proxy.retrieveAttributeValue("journalFileSize");
         }

         public int getJournalMaxIO()
         {
            return (Integer)proxy.retrieveAttributeValue("journalMaxIO");
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

         public boolean isAsyncConnectionExecutionEnabled()
         {
            return (Boolean)proxy.retrieveAttributeValue("asyncConnectionExecutionEnabled");
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

         public String[] listRemoteAddresses(final String ipAddress) throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(final String connectionID) throws Exception
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

         public boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
         {
            return (Boolean)proxy.invokeOperation("rollbackPreparedTransaction", transactionAsBase64);
         }

         public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
         {
            proxy.invokeOperation("sendQueueInfoToQueue", queueName, address);
         }

         public void setMessageCounterMaxDayCount(final int count) throws Exception
         {
            proxy.invokeOperation("setMessageCounterMaxDayCount", count);
         }

         public void setMessageCounterSamplePeriod(final long newPeriod) throws Exception
         {
            proxy.invokeOperation("setMessageCounterSamplePeriod", newPeriod);
         }

         public int getJournalBufferSize()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalBufferSize");
         }

         public int getJournalBufferTimeout()
         {
            return (Integer)proxy.retrieveAttributeValue("JournalBufferTimeout");
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
         
         public void addSecuritySettings(String addressMatch,
                                         String sendRoles,
                                         String consumeRoles,
                                         String createDurableQueueRoles,
                                         String deleteDurableQueueRoles,
                                         String createTempQueueRoles,
                                         String deleteTempQueueRoles,
                                         String manageRoles) throws Exception
         {
            proxy.invokeOperation("addSecuritySettings", addressMatch, 
                                  sendRoles, consumeRoles,
                                  createDurableQueueRoles, deleteDurableQueueRoles,
                                  createTempQueueRoles, deleteTempQueueRoles,
                                  manageRoles);
         }
         
         public void removeSecuritySettings(String addressMatch) throws Exception {
            proxy.invokeOperation("removeSecuritySettings", addressMatch); 
         }

         public Set<Role> getSecuritySettings(String addressMatch) throws Exception
         {
            return (Set<Role>)proxy.invokeOperation("removeSecuritySettings", addressMatch);
         }
         
         public Object[] getRoles(String addressMatch) throws Exception
         {
            return (Object[])proxy.invokeOperation("getRoles", addressMatch);
         }
         
         public String getRolesAsJSON(String addressMatch) throws Exception
         {
            return (String)proxy.invokeOperation("getRolesAsJSON", addressMatch);
         }

         public void addAddressSettings(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch,
                                        @Parameter(desc = "the dead letter address setting", name = "DLA") String DLA, 
                                        @Parameter(desc = "the expiry address setting", name = "expiryAddress") String expiryAddress, 
                                        @Parameter(desc = "are any queues created for this address a last value queue", name = "lastValueQueue") boolean lastValueQueue,
                                        @Parameter(desc = "the delivery attempts", name = "deliveryAttempts") int deliveryAttempts, 
                                        @Parameter(desc = "the max size in bytes", name = "maxSizeBytes") long maxSizeBytes, 
                                        @Parameter(desc = "the page size in bytes", name = "pageSizeBytes") int pageSizeBytes, 
                                        @Parameter(desc = "the redelivery delay", name = "redeliveryDelay") long redeliveryDelay,
                                        @Parameter(desc = "the redistribution delay", name = "redistributionDelay") long redistributionDelay,
                                        @Parameter(desc = "do we send to the DLA when there is no where to route the message", name = "sendToDLAOnNoRoute") boolean sendToDLAOnNoRoute,
                                        @Parameter(desc = "the ploicy to use when the address is full", name = "addressFullMessagePolicy") String addressFullMessagePolicy) throws Exception
         {
            proxy.invokeOperation("addAddressSettings", addressMatch, DLA, expiryAddress, lastValueQueue, deliveryAttempts, maxSizeBytes, pageSizeBytes, redeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy);
         }

         public AddressSettings getAddressSettings(String address) throws Exception
         {
            String res = (String)proxy.invokeOperation("getAddressSettingsAsJSON", address);
            JSONObject object = new JSONObject(res);
            AddressSettings settings = new AddressSettings();
            settings.setDeadLetterAddress(SimpleString.toSimpleString(object.getString("DLA")));
            settings.setExpiryAddress(SimpleString.toSimpleString(object.getString("expiryAddress")));
            settings.setLastValueQueue(object.getBoolean("lastValueQueue"));
            settings.setMaxDeliveryAttempts(object.getInt("maxDeliveryAttempts"));
            settings.setMaxSizeBytes(object.getLong("maxSizeBytes"));
            settings.setPageSizeBytes(object.getInt("pageSizeBytes"));
            settings.setRedeliveryDelay(object.getLong("redeliveryDelay"));
            settings.setRedistributionDelay(object.getLong("redistributionDelay"));
            settings.setSendToDLAOnNoRoute(object.getBoolean("sendToDLAOnNoRoute"));
            settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.valueOf(object.getString("addressFullMessagePolicy")));
            return settings;
         }

         public void removeAddressSettings(String addressMatch) throws Exception
         {
            proxy.invokeOperation("removeAddressSettings", addressMatch);
         }

         public String getAddressSettingsAsJSON(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception
         {
            return (String)proxy.invokeOperation("getAddressSettingsAsJSON", addressMatch);
         }
      };
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
