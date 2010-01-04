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

import javax.management.MBeanOperationInfo;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.server.management.Operation;
import org.hornetq.core.server.management.Parameter;

/**
 * A HornetQServerControl is used to manage HornetQ servers.
 */
public interface HornetQServerControl
{
   // Attributes ----------------------------------------------------

   /**
    * @see Configuration#getBackupConnectorName()
    */
   String getBackupConnectorName();

   /**
    * Returns this server's version.
    */
   String getVersion();

   /**
    * Returns the number of connections connected to this server.
    */
   int getConnectionCount();

   /**
    * Return whether this server is started.
    */
   boolean isStarted();

   /**
    * @see Configuration#getInterceptorClassNames()
    */
   String[] getInterceptorClassNames();

   /**
    * @see Configuration#isClustered()
    */
   boolean isClustered();

   /**
    * @see Configuration#getScheduledThreadPoolMaxSize()
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * @see Configuration#getThreadPoolMaxSize()
    */
   int getThreadPoolMaxSize();

   /**
    * @see Configuration#getSecurityInvalidationInterval()
    */
   long getSecurityInvalidationInterval();

   /**
    * @see Configuration#isSecurityEnabled()
    */
   boolean isSecurityEnabled();

   /**
    * @see Configuration#getBindingsDirectory()
    */
   String getBindingsDirectory();

   /**
    * @see Configuration#getJournalDirectory()
    */
   String getJournalDirectory();

   /**
    * @see Configuration#getJournalType()
    */
   String getJournalType();

   /**
    * @see Configuration#isJournalSyncTransactional()
    */
   boolean isJournalSyncTransactional();

   /**
    * @see Configuration#isJournalSyncNonTransactional()()
    */
   boolean isJournalSyncNonTransactional();

   /**
    * @see Configuration#getJournalFileSize()
    */
   int getJournalFileSize();

   /**
    * @see Configuration#getJournalMinFiles()
    */
   int getJournalMinFiles();

   /**
    * Returns the maximum number of write requests that can be in the journal at any given time.
    *
    * @see Configuration#getJournalMaxIO_AIO()
    * @see Configuration#getJournalMaxIO_NIO()
    */
   int getJournalMaxIO();

   /**
    * Returns the size of the internal buffer on the journal.
    * 
    * @see Configuration#getJournalBufferSize_AIO()
    * @see Configuration#getJournalBufferSize_NIO()
    */
   int getJournalBufferSize();

   /**
    * Returns the timeout (in nanoseconds) used to flush internal buffers on the journal.
    * 
    * @see Configuration#getJournalBufferTimeout_AIO()
    * @see Configuration#getJournalBufferTimeout_NIO()
    */
   int getJournalBufferTimeout();

   /**
    * @see Configuration#getJournalCompactMinFiles()
    */
   int getJournalCompactMinFiles();

   /**
    * @see Configuration#getJournalCompactPercentage()
    */
   int getJournalCompactPercentage();

   /**
    * @see Configuration#isPersistenceEnabled()
    */
   boolean isPersistenceEnabled();

   /**
    * @see Configuration#isCreateBindingsDir()
    */
   boolean isCreateBindingsDir();

   /**
    * @see Configuration#isCreateJournalDir()
    */
   boolean isCreateJournalDir();

   /**
    * @see Configuration#isMessageCounterEnabled()
    */
   boolean isMessageCounterEnabled();

   /**
    * @see Configuration#getMessageCounterMaxDayHistory()
    */
   int getMessageCounterMaxDayCount();

   /**
    * @see Configuration#setMessageCounterMaxDayHistory(int)
    */
   void setMessageCounterMaxDayCount(int count) throws Exception;

   /**
    * @see Configuration#getMessageCounterSamplePeriod()
    */
   long getMessageCounterSamplePeriod();

   /**
    * @see Configuration#setMessageCounterSamplePeriod(long)
    */
   void setMessageCounterSamplePeriod(long newPeriod) throws Exception;

   /**
    * @see Configuration#isBackup()
    */
   boolean isBackup();

   /**
    * @see Configuration#isSharedStore()
    */
   boolean isSharedStore();

   /**
    * @see Configuration#getPagingDirectory()
    */
   String getPagingDirectory();

   /**
    * @see Configuration#isPersistDeliveryCountBeforeDelivery()
    */
   boolean isPersistDeliveryCountBeforeDelivery();

   /**
    * @see Configuration#getConnectionTTLOverride()
    */
   long getConnectionTTLOverride();

   /**
    * @see Configuration#getManagementAddress()
    */
   String getManagementAddress();

   /**
    * @see Configuration#getManagementNotificationAddress()
    */
   String getManagementNotificationAddress();

   /**
    * @see Configuration#getIDCacheSize()
    */
   int getIDCacheSize();

   /**
    * @see Configuration#isPersistIDCache()
    */
   boolean isPersistIDCache();

   /**
    * @see Configuration#getLargeMessagesDirectory()
    */
   String getLargeMessagesDirectory();

   /**
    * @see Configuration#isWildcardRoutingEnabled()
    */
   boolean isWildcardRoutingEnabled();

   /**
    * @see Configuration#getTransactionTimeout()
    */
   long getTransactionTimeout();

   /**
    * @see Configuration#getTransactionTimeoutScanPeriod()
    */
   long getTransactionTimeoutScanPeriod();

   /**
    * @see Configuration#getMessageExpiryScanPeriod()
    */
   long getMessageExpiryScanPeriod();

   /**
    * @see Configuration#getMessageExpiryThreadPriority()
    */
   long getMessageExpiryThreadPriority();

   /**
    * @see Configuration#getConnectorConfigurations()
    */
   Object[] getConnectors() throws Exception;

   /**
    * Returns the connectors configured for this server using JSON serialization.
    * 
    * @see Configuration#getConnectorConfigurations()
    */
   String getConnectorsAsJSON() throws Exception;

   /**
    * Returns the addresses created on this server.
    */
   String[] getAddressNames();

   /**
    * Returns the names of the queues created on this server.
    */
   String[] getQueueNames();

   // Operations ----------------------------------------------------

   /**
    * Create a durable queue.
    * <br>
    * This method throws a {@link HornetQException#QUEUE_EXISTS}) exception if the queue already exits.
    * 
    * @param address address to bind the queue to
    * @param name name of the queue
    */
   @Operation(desc = "Create a queue with the specified address", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name) throws Exception;

   /**
    * Create a queue.
    * <br>
    * This method throws a {@link HornetQException#QUEUE_EXISTS}) exception if the queue already exits.
    * 
    * @param address address to bind the queue to
    * @param name name of the queue
    * @param filter of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Create a queue", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Create a queue.
    * <br>
    * This method throws a {@link HornetQException#QUEUE_EXISTS}) exception if the queue already exits.
    * 
    * @param address address to bind the queue to
    * @param name name of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Create a queue with the specified address, name and durability", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Deploy a durable queue.
    * <br>
    * This method will do nothing if the queue with the given name already exists on the server.
    * 
    * @param address address to bind the queue to
    * @param name name of the queue
    * @param filter of the queue
    */
   @Operation(desc = "Deploy a queue", impact = MBeanOperationInfo.ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue")String filter) throws Exception;

   /**
    * Deploy a queue.
    * <br>
    * This method will do nothing if the queue with the given name already exists on the server.
    * 
    * @param address address to bind the queue to
    * @param name name of the queue
    * @param filter of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Deploy a queue", impact = MBeanOperationInfo.ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Destroys the queue corresponding to the specified name.
    */
   @Operation(desc = "Destroy a queue", impact = MBeanOperationInfo.ACTION)
   void destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name) throws Exception;

   /**
    * Enables message counters for this server.
    */
   @Operation(desc = "Enable message counters", impact = MBeanOperationInfo.ACTION)
   void enableMessageCounters() throws Exception;

   /**
    * Disables message counters for this server.
    */
   @Operation(desc = "Disable message counters", impact = MBeanOperationInfo.ACTION)
   void disableMessageCounters() throws Exception;

   /**
    * Reset all message counters.
    */
   @Operation(desc = "Reset all message counters", impact = MBeanOperationInfo.ACTION)
   void resetAllMessageCounters() throws Exception;

   /**
    * Reset histories for all message counters.
    */
   @Operation(desc = "Reset all message counters history", impact = MBeanOperationInfo.ACTION)
   void resetAllMessageCounterHistories() throws Exception;

   /**
    * List all the prepared transaction, sorted by date, oldest first.
    * <br>
    * The Strings are Base-64 representation of the transaction XID and can be
    * used to heuristically commit or rollback the transactions.
    * 
    * @see #commitPreparedTransaction(String)
    * @see #rollbackPreparedTransaction(String)
    */
   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first")
   String[] listPreparedTransactions() throws Exception;

   /**
    * List transactions which have been heuristically committed.
    */
   String[] listHeuristicCommittedTransactions() throws Exception;

   /**
    * List transactions which have been heuristically rolled back.
    */
   String[] listHeuristicRolledBackTransactions() throws Exception;

   /**
    * Heuristically commits a prepared transaction.
    * 
    * @param transactionAsBase64 base 64 representation of a prepare transaction
    * @return {@code true} if the transaction was successfully committed, {@code false} else
    * 
    * @see #listPreparedTransactions()
    */
   @Operation(desc = "Commit a prepared transaction")
   boolean commitPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   /**
    * Heuristically rolls back a prepared transaction.
    * 
    * @param transactionAsBase64 base 64 representation of a prepare transaction
    * @return {@code true} if the transaction was successfully rolled back, {@code false} else
    * 
    * @see #listPreparedTransactions()
    */
   @Operation(desc = "Rollback a prepared transaction")
   boolean rollbackPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   /**
    * Lists the addresses of all the clients connected to this address.
    */
   @Operation(desc = "List the client addresses", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses() throws Exception;

   /**
    * Lists the addresses of the clients connected to this address which matches the specified IP address.
    */
   @Operation(desc = "List the client addresses which match the given IP Address", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Closes all the connections of clients connected to this server which matches the specified IP address.
    */
   @Operation(desc = "Closes all the connections for the given IP Address", impact = MBeanOperationInfo.INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Lists all the IDs of the connections connected to this server.
    */
   @Operation(desc = "List all the connection IDs", impact = MBeanOperationInfo.INFO)
   String[] listConnectionIDs() throws Exception;

   /**
    * Lists all the sessions IDs for the specified connection ID.
    */
   @Operation(desc = "List the sessions for the given connectionID", impact = MBeanOperationInfo.INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception;

   /**
    * This method is used by HornetQ clustering and must not be called by HornetQ clients.
    */
   void sendQueueInfoToQueue(String queueName, String address) throws Exception;

}
