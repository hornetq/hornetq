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

package org.hornetq.core.persistence;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;

/**
 *
 * A StorageManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public interface StorageManager extends HornetQComponent
{

   /** Get the context associated with the thread for later reuse */
   OperationContext getContext();

   void lineUpContext();

   /** It just creates an OperationContext without associating it */
   OperationContext newContext(Executor executor);

   OperationContext newSingleThreadContext();

   /** Set the context back to the thread */
   void setContext(OperationContext context);

   /**
    *
    * @param ioCriticalError is the server being stopped due to an IO critical error
    */
   void stop(boolean ioCriticalError) throws Exception;

   // Message related operations

   void pageClosed(SimpleString storeName, int pageNumber);

   void pageDeleted(SimpleString storeName, int pageNumber);

   void pageWrite(PagedMessage message, int pageNumber);

   boolean isReplicated();

   void afterCompleteOperations(IOAsyncTask run);

   /** Block until the operations are done.
    *  Warning: Don't use it inside an ordered executor, otherwise the system may lock up
    *           in case of the pools are full
    * @throws Exception */
   boolean waitOnOperations(long timeout) throws Exception;

   /** Block until the operations are done.
    *  Warning: Don't use it inside an ordered executor, otherwise the system may lock up
    *           in case of the pools are full
    * @throws Exception */
   void waitOnOperations() throws Exception;

   void clearContext();

   long generateUniqueID();

   long getCurrentUniqueID();

   /** Confirms that a large message was finished */
   void confirmPendingLargeMessageTX(Transaction transaction, long messageID, long recordID) throws Exception;

   /** Confirms that a large message was finished */
   void confirmPendingLargeMessage(long recordID) throws Exception;

   void storeMessage(ServerMessage message) throws Exception;

   void storeReference(long queueID, long messageID, boolean last) throws Exception;

   void deleteMessage(long messageID) throws Exception;

   void storeAcknowledge(long queueID, long messageID) throws Exception;

   void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception;

   void updateDeliveryCount(MessageReference ref) throws Exception;

   void updateScheduledDeliveryTime(MessageReference ref) throws Exception;

   void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateID(long recordID) throws Exception;

   void storeMessageTransactional(long txID, ServerMessage message) throws Exception;

   void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception;

   void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception;

   void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception;

   void storeDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void updateDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception;

   LargeServerMessage createLargeMessage();

   /**
    *
    * @param id
    * @param message This is a temporary message that holds the parsed properties.
    *        The remoting layer can't create a ServerMessage directly, then this will be replaced.
    * @return
    * @throws Exception
    */
   LargeServerMessage createLargeMessage(long id, MessageInternal message) throws Exception;

   void prepare(long txID, Xid xid) throws Exception;

   void commit(long txID) throws Exception;

   void commit(long txID, boolean lineUpContext) throws Exception;

   void rollback(long txID) throws Exception;

   void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception;

   void updatePageTransaction(long txID, PageTransactionInfo pageTransaction,  int depage) throws Exception;

   /** FIXME Unused */
   void updatePageTransaction(PageTransactionInfo pageTransaction,  int depage) throws Exception;

   void deletePageTransactional(long recordID) throws Exception;

   JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                             final PagingManager pagingManager,
                                             final ResourceManager resourceManager,
                                             final Map<Long, Queue> queues,
                                             Map<Long, QueueBindingInfo> queueInfos,
                                             final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                             final Set<Pair<Long, Long>> pendingLargeMessages) throws Exception;

   long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception;

   void deleteHeuristicCompletion(long id) throws Exception;

   // Bindings related operations

   void addQueueBinding(Binding binding) throws Exception;

   void deleteQueueBinding(long queueBindingID) throws Exception;

   JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos, List<GroupingInfo> groupingInfos) throws Exception;

   // grouping related operations
   void addGrouping(GroupBinding groupBinding) throws Exception;

   void deleteGrouping(GroupBinding groupBinding) throws Exception;

   void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception;

   void deleteAddressSetting(SimpleString addressMatch) throws Exception;

   List<PersistedAddressSetting> recoverAddressSettings() throws Exception;

   void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception;

   void deleteSecurityRoles(SimpleString addressMatch) throws Exception;

   List<PersistedRoles> recoverPersistedRoles() throws Exception;

   /**
    * @return The ID with the stored counter
    */
   long storePageCounter(long txID, long queueID, long value) throws Exception;

   void deleteIncrementRecord(long txID, long recordID) throws Exception;

   void deletePageCounter(long txID, long recordID) throws Exception;

   /**
    * @return the ID with the increment record
    * @throws Exception
    */
   long storePageCounterInc(long txID, long queueID, int add) throws Exception;

   /**
    * @return the ID with the increment record
    * @throws Exception
    */
   long storePageCounterInc(long queueID, int add) throws Exception;

   /**
    * @return the bindings journal
    */
   Journal getBindingsJournal();

   /**
    * @return the message journal
    */
   Journal getMessageJournal();

   /**
    * @param replicationManager
    * @param pagingManager
    * @param nodeID
    * @param clusterConnection
    * @param pair
    * @throws Exception
    */
   void startReplication(ReplicationManager replicationManager, PagingManager pagingManager, String nodeID,
      ClusterConnection clusterConnection, Pair<TransportConfiguration, TransportConfiguration> pair)
      throws Exception;

   /**
    * Adds message to page if we are paging.
    * @return whether we added the message to a page or not.
    */
   boolean addToPage(PagingManager pagingManager,
      SimpleString address,
      ServerMessage message,
      RoutingContext ctx,
      RouteContextList listCtx) throws Exception;
}
