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

package org.hornetq.core.persistence.impl.nullpm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUID;

/**
 * 
 * A NullStorageManager
 * 
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NullStorageManager implements StorageManager
{
   private final AtomicLong idSequence = new AtomicLong(0);
   
   private UUID id;

   private volatile boolean started;
   
   public UUID getPersistentID()
   {
      return id;
   }
   
   public void setPersistentID(final UUID id)
   {
      this.id = id;
   }
   
   public void sync()
   {
      // NO OP
   }

   public void addQueueBinding(final Binding queueBinding) throws Exception
   {
   }

   public void deleteQueueBinding(long queueBindingID) throws Exception
   {
   }

   public void commit(final long txID) throws Exception
   {
   }

   public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos, List<GroupingInfo> groupingInfos) throws Exception
   {
      return new JournalLoadInformation();
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
   }

   public void rollback(final long txID) throws Exception
   {
   }

   public void storeReference(final long queueID, final long messageID) throws Exception
   {
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
   }

   public void storeMessageReferenceScheduled(final long queueID, final long messageID, final long scheduledDeliveryTime) throws Exception
   {
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageiD) throws Exception
   {
   }

   public void deleteMessage(final long messageID) throws Exception
   {
   }

   public void deletePageTransactional(final long txID, final long messageID) throws Exception
   {
   }

   public void storeMessage(final ServerMessage message) throws Exception
   {
   }

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
   }

   public void updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
   }

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
   }

   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
   }

   public void updateDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
   }
   
   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      return generateUniqueID();
   }
   
   public void deleteHeuristicCompletion(final long txID) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#createLargeMessageStorage(long, int, int)
    */
   public LargeServerMessage createLargeMessage()
   {
      return new NullStorageLargeServerMessage();
   }

   public LargeServerMessage createLargeMessage(long id, byte[] header)
   {
      NullStorageLargeServerMessage largeMessage = new NullStorageLargeServerMessage();

      HornetQBuffer headerBuffer = ChannelBuffers.wrappedBuffer(header);

      largeMessage.decodeHeadersAndProperties(headerBuffer);

      largeMessage.setMessageID(id);

      return largeMessage;
   }


   public long generateUniqueID()
   {
      long id = idSequence.getAndIncrement();
      
      return id;
   }
   
   public long getCurrentUniqueID()
   {
      return idSequence.get();
   }
   
   public void setUniqueIDSequence(final long id)
   {
      idSequence.set(id);
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Already started");
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Not started");
      }
      
      id = null;
      
      idSequence.set(0);

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public void deleteMessageTransactional(final long txID, final long messageID, final long queueID) throws Exception
   {
   }
   
   public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
                                  PagingManager pagingManager,
                                  ResourceManager resourceManager,
                                  Map<Long, Queue> queues,
                                  Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      return new JournalLoadInformation();
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#loadInternalOnly()
    */
   public JournalLoadInformation[] loadInternalOnly() throws Exception
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#isReplicated()
    */
   public boolean isReplicated()
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#completeReplication()
    */
   public void completeOperations()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageClosed(org.hornetq.utils.SimpleString, int)
    */
   public void pageClosed(SimpleString storeName, int pageNumber)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageDeleted(org.hornetq.utils.SimpleString, int)
    */
   public void pageDeleted(SimpleString storeName, int pageNumber)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageWrite(org.hornetq.core.paging.PagedMessage, int)
    */
   public void pageWrite(PagedMessage message, int pageNumber)
   {
   }

   public void addGrouping(GroupBinding groupBinding) throws Exception
   {
   }

   public void deleteGrouping(GroupBinding groupBinding) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#blockOnReplication(long)
    */
   public void waitOnOperations(long timeout) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#setReplicator(org.hornetq.core.replication.ReplicationManager)
    */
   public void setReplicator(ReplicationManager replicator)
   {
      throw new IllegalStateException("Null Persistence should never be used as replicated");
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#afterCompleteOperations(org.hornetq.core.journal.IOCompletion)
    */
   public void afterCompleteOperations(IOAsyncTask run)
   {
      run.done();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#waitOnOperations()
    */
   public void waitOnOperations() throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#getContext()
    */
   public OperationContext getContext()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#newContext()
    */
   public OperationContext newContext(Executor executor)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#setContext(org.hornetq.core.persistence.OperationContext)
    */
   public void setContext(OperationContext context)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#clearContext()
    */
   public void clearContext()
   {
   }

}
