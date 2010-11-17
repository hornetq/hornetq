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
import java.util.concurrent.Executor;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.utils.UUID;

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

   /** It just creates an OperationContext without associating it */
   OperationContext newContext(Executor executor);

   /** Set the context back to the thread */
   void setContext(OperationContext context);

   // Message related operations

   void pageClosed(SimpleString storeName, int pageNumber);

   void pageDeleted(SimpleString storeName, int pageNumber);

   void pageWrite(PagedMessage message, int pageNumber);

   boolean isReplicated();

   void afterCompleteOperations(IOAsyncTask run);

   /** Block until the operations are done. 
    * @throws Exception */
   void waitOnOperations(long timeout) throws Exception;

   /** Block until the operations are done. 
    * @throws Exception */
   void waitOnOperations() throws Exception;

   void clearContext();

   UUID getPersistentID();

   void setPersistentID(UUID id) throws Exception;

   long generateUniqueID();

   long getCurrentUniqueID();

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

   void deleteMessageTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void updateDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception;

   LargeServerMessage createLargeMessage();

   LargeServerMessage createLargeMessage(long id, byte[] header);

   void prepare(long txID, Xid xid) throws Exception;

   void commit(long txID) throws Exception;

   void rollback(long txID) throws Exception;

   void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception;
   
   void updatePageTransaction(long txID, PageTransactionInfo pageTransaction,  int depage) throws Exception;
   
   void updatePageTransaction(PageTransactionInfo pageTransaction,  int depage) throws Exception;

   void deletePageTransactional(long recordID) throws Exception;

   /** This method is only useful at the backup side. We only load internal structures making the journals ready for
    *  append mode on the backup side. */
   JournalLoadInformation[] loadInternalOnly() throws Exception;

   JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                             final PagingManager pagingManager,
                                             final ResourceManager resourceManager,
                                             final Map<Long, Queue> queues,
                                             Map<Long, QueueBindingInfo> queueInfos,
                                             final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception;

   long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception;

   void deleteHeuristicCompletion(long id) throws Exception;

   // Bindings related operations

   void addQueueBinding(Binding binding) throws Exception;

   void deleteQueueBinding(long queueBindingID) throws Exception;

   JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos, List<GroupingInfo> groupingInfos) throws Exception;

   // grouping relateed operations
   void addGrouping(GroupBinding groupBinding) throws Exception;

   void deleteGrouping(GroupBinding groupBinding) throws Exception;
   
   void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception;
   
   void deleteAddressSetting(SimpleString addressMatch) throws Exception;
   
   List<PersistedAddressSetting> recoverAddressSettings() throws Exception;
   
   void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception;
   
   void deleteSecurityRoles(SimpleString addressMatch) throws Exception;

   List<PersistedRoles> recoverPersistedRoles() throws Exception;
}
