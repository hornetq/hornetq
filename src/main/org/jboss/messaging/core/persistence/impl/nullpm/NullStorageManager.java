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

package org.jboss.messaging.core.persistence.impl.nullpm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.persistence.QueueBindingInfo;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;

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

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.persistence.StorageManager#createLargeMessageStorage(long, int, int)
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
