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

import javax.transaction.xa.Xid;

import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
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
   // Message related operations
   
   UUID getPersistentID();
   
   void setPersistentID(UUID id) throws Exception;

   long generateUniqueID();
   
   long getCurrentUniqueID();

   void storeMessage(ServerMessage message) throws Exception;
   
   void storeReference(long queueID, long messageID) throws Exception;

   void deleteMessage(long messageID) throws Exception;

   void storeAcknowledge(long queueID, long messageID) throws Exception;

   void updateDeliveryCount(MessageReference ref) throws Exception;

   void updateScheduledDeliveryTime(MessageReference ref) throws Exception;

   void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateID(long recordID) throws Exception;

   void storeMessageTransactional(long txID, ServerMessage message) throws Exception;
   
   void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception;

   void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception;

   void deleteMessageTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void updateDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception;

   LargeServerMessage createLargeMessage();

   void prepare(long txID, Xid xid) throws Exception;

   void commit(long txID) throws Exception;

   void rollback(long txID) throws Exception;

   void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception;

   void deletePageTransactional(long txID, long recordID) throws Exception;

   long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception;
   
   void deleteHeuristicCompletion(long id) throws Exception;
   
   void loadMessageJournal(PagingManager pagingManager,
                           ResourceManager resourceManager,
                           Map<Long, Queue> queues,
                           Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception;

   // Bindings related operations

   void addQueueBinding(Binding binding) throws Exception;
   
   void deleteQueueBinding(long queueBindingID) throws Exception;
   
   void loadBindingJournal(List<QueueBindingInfo> queueBindingInfos) throws Exception;
}
