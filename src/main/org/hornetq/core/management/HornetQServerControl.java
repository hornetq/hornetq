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

package org.hornetq.core.management;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

import org.hornetq.core.config.Configuration;

/**
 * This interface describes the core management interface exposed by the server
 */
public interface HornetQServerControl
{
   // Attributes ----------------------------------------------------

   String getBackupConnectorName();

   String getVersion();

   int getConnectionCount();
   
   boolean isStarted();

   String[] getInterceptorClassNames();

   boolean isClustered();

   int getScheduledThreadPoolMaxSize();

   int getThreadPoolMaxSize();

   long getSecurityInvalidationInterval();

   boolean isSecurityEnabled();

   String getBindingsDirectory();

   String getJournalDirectory();

   String getJournalType();

   boolean isJournalSyncTransactional();

   boolean isJournalSyncNonTransactional();

   int getJournalFileSize();

   int getJournalMinFiles();

   int getJournalMaxAIO();
   
   int getJournalCompactMinFiles();
   
   int getJournalCompactPercentage();
   
   boolean isPersistenceEnabled();

   boolean isCreateBindingsDir();

   boolean isCreateJournalDir();

   Configuration getConfiguration();

   boolean isMessageCounterEnabled();

   int getMessageCounterMaxDayCount();

   void setMessageCounterMaxDayCount(int count) throws Exception;

   long getMessageCounterSamplePeriod();

   void setMessageCounterSamplePeriod(long newPeriod) throws Exception;

   boolean isBackup();
   
   boolean isSharedStore();

   int getAIOBufferSize();

   int getAIOBufferTimeout();

   String getPagingDirectory();

   boolean isPersistDeliveryCountBeforeDelivery();

   long getConnectionTTLOverride();

   String getManagementAddress();

   String getManagementNotificationAddress();

   long getManagementRequestTimeout();

   int getIDCacheSize();

   boolean isPersistIDCache();

   String getLargeMessagesDirectory();

   boolean isWildcardRoutingEnabled();

   long getTransactionTimeout();

   long getTransactionTimeoutScanPeriod();

   long getMessageExpiryScanPeriod();

   long getMessageExpiryThreadPriority();

   Object[] getConnectors() throws Exception;

   String getConnectorsAsJSON() throws Exception;
   
   String[] getAddressNames();

   String[] getQueueNames();

   // Operations ----------------------------------------------------

   @Operation(desc = "Create a queue with the specified address", impact = ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name) throws Exception;
   
   @Operation(desc = "Create a queue", impact = ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;
   
   @Operation(desc = "Create a queue with the specified address, name and durability", impact = ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   @Operation(desc = "Deploy a queue", impact = ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    String filterString) throws Exception;

   @Operation(desc = "Deploy a queue", impact = ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   @Operation(desc = "Destroy a queue", impact = ACTION)
   void destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name) throws Exception;

   @Operation(desc = "Enable message counters", impact = ACTION)
   void enableMessageCounters() throws Exception;

   @Operation(desc = "Disable message counters", impact = ACTION)
   void disableMessageCounters() throws Exception;

   @Operation(desc = "Reset all message counters", impact = ACTION)
   void resetAllMessageCounters() throws Exception;

   @Operation(desc = "Reset all message counters history", impact = ACTION)
   void resetAllMessageCounterHistories() throws Exception;

   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first")
   String[] listPreparedTransactions() throws Exception;

   String[] listHeuristicCommittedTransactions() throws Exception;

   String[] listHeuristicRolledBackTransactions() throws Exception;

   @Operation(desc = "Commit a prepared transaction")
   boolean commitPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   @Operation(desc = "Rollback a prepared transaction")
   boolean rollbackPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   @Operation(desc = "List the client addresses", impact = INFO)
   String[] listRemoteAddresses() throws Exception;

   @Operation(desc = "List the client addresses which match the given IP Address", impact = INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   @Operation(desc = "Closes all the connections for the given IP Address", impact = INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   @Operation(desc = "List all the connection IDs", impact = INFO)
   String[] listConnectionIDs() throws Exception;

   @Operation(desc = "List the sessions for the given connectionID", impact = INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception;

   void sendQueueInfoToQueue(String queueName, String address) throws Exception;

}
