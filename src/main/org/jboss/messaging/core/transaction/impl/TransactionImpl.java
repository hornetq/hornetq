/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.transaction.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionSynchronization;
import org.jboss.messaging.util.SimpleString;

/**
 * A TransactionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class TransactionImpl implements Transaction
{
   private List<TransactionSynchronization> syncs;
   
   private static final Logger log = Logger.getLogger(TransactionImpl.class);

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final PagingManager pagingManager;

   private final List<MessageReference> refsToAdd = new ArrayList<MessageReference>();

   private final List<MessageReference> acknowledgements = new ArrayList<MessageReference>();

   private final List<ServerMessage> pagedMessages = new ArrayList<ServerMessage>();

   private PageTransactionInfo pageTransaction;

   private final Xid xid;

   private final long id;

   private volatile State state = State.ACTIVE;

   private volatile boolean containsPersistent;

   private MessagingException messagingException;

   private final Object timeoutLock = new Object();

   private final long createTime;

   public TransactionImpl(final StorageManager storageManager, final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      if (postOffice == null)
      {
         pagingManager = null;
      }
      else
      {
         pagingManager = postOffice.getPagingManager();
      }

      xid = null;

      id = storageManager.generateUniqueID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager, final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      if (postOffice == null)
      {
         pagingManager = null;
      }
      else
      {
         pagingManager = postOffice.getPagingManager();
      }

      this.xid = xid;

      id = storageManager.generateUniqueID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager, final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.xid = xid;

      this.id = id;

      if (postOffice == null)
      {
         pagingManager = null;
      }
      else
      {
         pagingManager = postOffice.getPagingManager();
      }

      createTime = System.currentTimeMillis();
   }

   // Transaction implementation
   // -----------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public void addMessage(final ServerMessage message) throws Exception
   {
      if (state != State.ACTIVE)
      {
         throw new IllegalStateException("Transaction is in invalid state " + state);
      }

      if (pagingManager.isPaging(message.getDestination()))
      {
         pagedMessages.add(message);
      }
      else
      {
         route(message);
      }
   }

   public List<MessageReference> timeout() throws Exception
   {
      // we need to synchronize with commit and rollback just in case they get called atthesame time
      synchronized (timeoutLock)
      {
         // if we've already rolled back or committed we don't need to do anything
         if (state == State.COMMITTED || state == State.ROLLBACK_ONLY || state == State.PREPARED)
         {
            return Collections.emptyList();
         }

         return doRollback();
      }
   }

   public long getCreateTime()
   {
      return createTime;
   }

   public void addAcknowledgement(final MessageReference acknowledgement) throws Exception
   {
      if (state != State.ACTIVE)
      {
         throw new IllegalStateException("Transaction is in invalid state " + state);
      }

      acknowledgements.add(acknowledgement);

      ServerMessage message = acknowledgement.getMessage();

      if (message.isDurable())
      {
         Queue queue = acknowledgement.getQueue();

         if (queue.isDurable())
         {
            // Need to lock on the message to prevent a race where the ack and
            // delete
            // records get recorded in the log in the wrong order

            // TODO For now - we just use synchronized - can probably do better
            // locking

            synchronized (message)
            {
               int count = message.decrementDurableRefCount();

               if (count == 0)
               {
                  storageManager.deleteMessageTransactional(id, queue.getPersistenceID(), message.getMessageID());
               }
               else
               {
                  storageManager.storeAcknowledgeTransactional(id, queue.getPersistenceID(), message.getMessageID());
               }

               containsPersistent = true;
            }
         }
      }
   }

   public void prepare() throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state != State.ACTIVE)
         {
            throw new IllegalStateException("Transaction is in invalid state " + state);
         }

         if (xid == null)
         {
            throw new IllegalStateException("Cannot prepare non XA transaction");
         }

         pageMessages();

         storageManager.prepare(id, xid);

         state = State.PREPARED;
         
         if (syncs != null)
         {
            for (TransactionSynchronization sync: syncs)
            {
               sync.afterPrepare();
            }
         }
      }
   }

   public void commit() throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state == State.ROLLBACK_ONLY)
         {
            if (messagingException != null)
            {
               throw messagingException;
            }
            else
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }

         }
         if (xid != null)
         {
            if (state != State.PREPARED)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else
         {
            if (state != State.ACTIVE)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }

         if (state != State.PREPARED)
         {
            pageMessages();
         }

         if (containsPersistent || xid != null)
         {
            storageManager.commit(id);
         }

         for (MessageReference ref : refsToAdd)
         {
            ref.getQueue().addLast(ref);
         }

         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order
         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }

         for (MessageReference reference : acknowledgements)
         {
            reference.getQueue().referenceAcknowledged(reference);
         }

         clear();

         state = State.COMMITTED;
         
         if (syncs != null)
         {
            for (TransactionSynchronization sync: syncs)
            {
               sync.afterCommit();
            }
         }
      }
   }

   public List<MessageReference> rollback(final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      LinkedList<MessageReference> toCancel;
      
      synchronized (timeoutLock)
      {
         if (xid != null)
         {
            if (state != State.PREPARED && state != State.ACTIVE)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else
         {
            if (state != State.ACTIVE && state != State.ROLLBACK_ONLY)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }

         toCancel = doRollback();

         state = State.ROLLEDBACK;
         
         if (syncs != null)
         {
            for (TransactionSynchronization sync: syncs)
            {
               sync.afterRollback();
            }
         }
      }

      return toCancel;
   }

   

   public int getAcknowledgementsCount()
   {
      return acknowledgements.size();
   }

   public void suspend()
   {
      if (state != State.ACTIVE)
      {
         throw new IllegalStateException("Can only suspend active transaction");
      }
      state = State.SUSPENDED;
   }

   public void resume()
   {
      if (state != State.SUSPENDED)
      {
         throw new IllegalStateException("Can only resume a suspended transaction");
      }
      state = State.ACTIVE;
   }

   public Transaction.State getState()
   {
      return state;
   }

   public Xid getXid()
   {
      return xid;
   }

   public boolean isEmpty()
   {
      return refsToAdd.isEmpty() && acknowledgements.isEmpty();
   }

   public boolean isContainsPersistent()
   {
      return containsPersistent;
   }

   public void markAsRollbackOnly(final MessagingException messagingException)
   {
      state = State.ROLLBACK_ONLY;

      this.messagingException = messagingException;
   }

   public void replay(final List<MessageReference> messages,
                      final List<MessageReference> acknowledgements,
                      final PageTransactionInfo pageTransaction) throws Exception
   {
      containsPersistent = true;
      refsToAdd.addAll(messages);
      this.acknowledgements.addAll(acknowledgements);
      this.pageTransaction = pageTransaction;

      if (this.pageTransaction != null)
      {
         pagingManager.addTransaction(this.pageTransaction);
      }

      state = State.PREPARED;
   }

   public void setContainsPersistent(final boolean containsPersistent)
   {
      this.containsPersistent = containsPersistent;
   }
   
   public void addSynchronization(final TransactionSynchronization sync)
   {
      checkCreateSyncs();
      
      syncs.add(sync);
   }

   public void removeSynchronization(final TransactionSynchronization sync)
   {
      checkCreateSyncs();
      
      syncs.remove(sync);
   }


   // Private
   // -------------------------------------------------------------------
   
   private LinkedList<MessageReference> doRollback() throws Exception
   {
      if (containsPersistent || xid != null)
      {
         storageManager.rollback(id);
      }

      if (state == State.PREPARED && pageTransaction != null)
      {
         pageTransaction.rollback();
      }

      LinkedList<MessageReference> toCancel = new LinkedList<MessageReference>();

      for (MessageReference ref : acknowledgements)
      {
         Queue queue = ref.getQueue();

         ServerMessage message = ref.getMessage();

         if (message.isDurable() && queue.isDurable())
         {
            message.incrementDurableRefCount();

         }
         toCancel.add(ref);
      }

      clear();

      return toCancel;
   }
   
   private void checkCreateSyncs()
   {
      if (syncs == null)
      {
         syncs = new ArrayList<TransactionSynchronization>();
      }
   }
   

   private List<MessageReference> route(final ServerMessage message) throws Exception
   {
      Long scheduledDeliveryTime = (Long)message.getProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);

      List<MessageReference> refs = postOffice.route(message);

      refsToAdd.addAll(refs);

      if (message.getDurableRefCount() != 0)
      {
         storageManager.storeMessageTransactional(id, message);

         containsPersistent = true;
      }

      if (scheduledDeliveryTime != null)
      {
         for (MessageReference ref : refs)
         {
            ref.setScheduledDeliveryTime(scheduledDeliveryTime);

            if (ref.getMessage().isDurable() && ref.getQueue().isDurable())
            {
               storageManager.updateScheduledDeliveryTimeTransactional(id, ref);
            }
         }
      }

      return refs;
   }

   private void pageMessages() throws Exception
   {
      HashSet<SimpleString> pagedDestinationsToSync = new HashSet<SimpleString>();

      boolean pagingPersistent = false;

      if (pagedMessages.size() != 0)
      {
         if (pageTransaction == null)
         {
            pageTransaction = new PageTransactionInfoImpl(id);
            // To avoid a race condition where depage happens before the transaction is completed, we need to inform the
            // pager about this transaction is being processed
            pagingManager.addTransaction(pageTransaction);
         }
      }

      for (ServerMessage message : pagedMessages)
      {
         // http://wiki.jboss.org/wiki/JBossMessaging2Paging
         // Explained under Transaction On Paging. (This is the item B)
         if (pagingManager.page(message, id))
         {
            if (message.isDurable())
            {
               // We only create pageTransactions if using persistent messages
               pageTransaction.increment();
               pagingPersistent = true;
               pagedDestinationsToSync.add(message.getDestination());
            }
         }
         else
         {
            // This could happen when the PageStore left the pageState
            route(message);
         }
      }

      if (pagingPersistent)
      {
         containsPersistent = true;
         if (pagedDestinationsToSync.size() > 0)
         {
            pagingManager.sync(pagedDestinationsToSync);
            storageManager.storePageTransaction(id, pageTransaction);
         }
      }
   }

   private void clear()
   {
      refsToAdd.clear();

      acknowledgements.clear();

      pagedMessages.clear();
   }
}
