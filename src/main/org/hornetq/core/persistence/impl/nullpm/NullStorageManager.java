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
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
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
 * A NullStorageManager
 * 
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NullStorageManager implements StorageManager
{
   private static final Logger log = Logger.getLogger(NullStorageManager.class);
   
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

   public void addQueueBinding(final Binding queueBinding) throws Exception
   {
   }

   public void deleteQueueBinding(long queueBindingID) throws Exception
   {
   }

   public void commit(final long txID) throws Exception
   {
   }

   public void loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos) throws Exception
   {

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
   
   public void loadMessageJournal(PagingManager pagingManager,
                                  ResourceManager resourceManager,
                                  Map<Long, Queue> queues,
                                  Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
   }

}
