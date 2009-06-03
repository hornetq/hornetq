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

package org.jboss.messaging.core.management;

import org.jboss.messaging.core.config.Configuration;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

/**
 * This interface describes the core management interface exposed by the server
 */
public interface MessagingServerControlMBean
{
   // Attributes ----------------------------------------------------

   public String getBackupConnectorName();

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

   boolean isCreateBindingsDir();

   boolean isCreateJournalDir();

   Configuration getConfiguration();

   boolean isMessageCounterEnabled();

   int getMessageCounterMaxDayCount();

   void setMessageCounterMaxDayCount(int count) throws Exception;

   long getMessageCounterSamplePeriod();

   void setMessageCounterSamplePeriod(long newPeriod) throws Exception;

   public boolean isBackup();

   public long getConnectionScanPeriod();
   
   int getAIOBufferSize();
   
   long getAIOBufferTimeout();
   
   public long getPagingMaxGlobalSizeBytes();

   public String getPagingDirectory();

   boolean isPersistDeliveryCountBeforeDelivery();

   long getQueueActivationTimeout();

   long getConnectionTTLOverride();

   String getManagementAddress();

   String getManagementNotificationAddress();

   long getManagementRequestTimeout();

   int getIDCacheSize();

   boolean isPersistIDCache();

   int getGlobalPageSize();

   String getLargeMessagesDirectory();

   boolean isWildcardRoutingEnabled();

   long getTransactionTimeout();

   long getTransactionTimeoutScanPeriod();

   long getMessageExpiryScanPeriod();

   long getMessageExpiryThreadPriority();
   
   Object[] getConnectors() throws Exception;

   // Operations ----------------------------------------------------

   @Operation(desc = "Create a queue with the specified address", impact = ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue")
   String address, @Parameter(name = "name", desc = "Name of the queue")
   String name) throws Exception;

   @Operation(desc = "Create a queue", impact = ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue")
   String address, @Parameter(name = "name", desc = "Name of the queue")
   String name, @Parameter(name = "filter", desc = "Filter of the queue")
   String filter, @Parameter(name = "durable", desc = "Is the queue durable?")
   boolean durable) throws Exception;

   @Operation(desc = "Deploy a queue", impact = ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue")
   String address, @Parameter(name = "name", desc = "Name of the queue")
   String name, String filterString) throws Exception;

   @Operation(desc = "Deploy a queue", impact = ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue")
   String address, @Parameter(name = "name", desc = "Name of the queue")
   String name, @Parameter(name = "filter", desc = "Filter of the queue")
   String filter, @Parameter(name = "durable", desc = "Is the queue durable?")
   boolean durable) throws Exception;

   @Operation(desc = "Destroy a queue", impact = ACTION)
   void destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy")
   String name) throws Exception;

   void enableMessageCounters() throws Exception;

   void disableMessageCounters() throws Exception;

   void resetAllMessageCounters() throws Exception;

   void resetAllMessageCounterHistories() throws Exception;

   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first")
   public String[] listPreparedTransactions() throws Exception;

   @Operation(desc = "Commit a prepared transaction")
   boolean commitPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64")
   String transactionAsBase64) throws Exception;

   @Operation(desc = "Rollback a prepared transaction")
   boolean rollbackPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64")
   String transactionAsBase64) throws Exception;

   @Operation(desc = "List the client addresses", impact = INFO)
   String[] listRemoteAddresses() throws Exception;

   @Operation(desc = "List the client addresses which match the given IP Address", impact = INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress")
   String ipAddress) throws Exception;

   @Operation(desc = "Closes all the connections for the given IP Address", impact = INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress")
   String ipAddress) throws Exception;

   @Operation(desc = "List all the connection IDs", impact = INFO)
   String[] listConnectionIDs() throws Exception;

   @Operation(desc = "List the sessions for the given connectionID", impact = INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID")
   String connectionID) throws Exception;

   void sendQueueInfoToQueue(String queueName, String address) throws Exception;


}
