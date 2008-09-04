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

package org.jboss.messaging.core.transaction.impl;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
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
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.Xid;
import java.util.*;

/**
 * A TransactionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class TransactionImpl implements Transaction
{
   private static final Logger log = Logger.getLogger(TransactionImpl.class);


   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final PagingManager pagingManager;

   private final List<MessageReference> refsToAdd = new ArrayList<MessageReference>();

   private final List<MessageReference> acknowledgements = new ArrayList<MessageReference>();

   private final List<ServerMessage> pagedMessages = new ArrayList<ServerMessage>();

   private PageTransactionInfoImpl pageTransaction;

   private final Xid xid;

   private final long id;

   private volatile State state = State.ACTIVE;

   private volatile boolean containsPersistent;

   private MessagingException messagingException;

   public TransactionImpl(final StorageManager storageManager,
                          final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      if (postOffice == null)
      {
         pagingManager = null;
      }
      else
      {
         this.pagingManager = postOffice.getPagingManager();
      }

      this.xid = null;

      this.id = storageManager.generateTransactionID();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager,
                          final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      if (postOffice == null)
      {
         pagingManager = null;
      }
      else
      {
         this.pagingManager = postOffice.getPagingManager();
      }


      this.xid = xid;

      this.id = storageManager.generateTransactionID();
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager,
                          final PostOffice postOffice)
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
         this.pagingManager = postOffice.getPagingManager();
      }
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

   public void addAcknowledgement(final MessageReference acknowledgement)
           throws Exception
   {
      if (state != State.ACTIVE)
      {
         throw new IllegalStateException("Transaction is in invalid state " + state);
      }

      acknowledgements.add(acknowledgement);

      ServerMessage message = acknowledgement.getMessage();

      if (message.decrementRefCount() == 0)
      {
         if (pagingManager != null)
         {
            pagingManager.messageDone(message);
         }
      }

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
                  storageManager.storeDeleteTransactional(id, message
                          .getMessageID());
               }
               else
               {
                  storageManager.storeAcknowledgeTransactional(id, queue
                          .getPersistenceID(), message.getMessageID());
               }

               containsPersistent = true;
            }
         }
      }
   }

   public void prepare() throws Exception
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

      if (containsPersistent)
      {
         storageManager.prepare(id, xid);
      }

      state = State.PREPARED;
   }

   public void commit() throws Exception
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

      if (containsPersistent)
      {
         storageManager.commit(id);
      }

      for (MessageReference ref : refsToAdd)
      {
         ref.getQueue().addLast(ref);
      }

      // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the transaction until all the messages were added to the queue
      // or else we could deliver the messages out of order
      if (pageTransaction != null)
      {
         pageTransaction.complete();
      }

      for (MessageReference reference : acknowledgements)
      {
         reference.getQueue().referenceAcknowledged(reference);
      }

      clear();

      state = State.COMMITTED;
   }

   public void rollback(final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
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

      if (containsPersistent)
      {
         storageManager.rollback(id);
      }

      Map<Queue, LinkedList<MessageReference>> queueMap = new HashMap<Queue, LinkedList<MessageReference>>();

      // We sort into lists - one for each queue involved.
      // Then we cancel back atomicly for each queue adding list on front to
      // guarantee ordering is preserved

      for (MessageReference ref : acknowledgements)
      {
         Queue queue = ref.getQueue();

         ServerMessage message = ref.getMessage();


         // Putting back the size on pagingManager, and reverting the counters
         if (message.incrementReference(message.isDurable() && queue.isDurable()) == 1)
         {
            pagingManager.addSize(message);
         }

         LinkedList<MessageReference> list = queueMap.get(queue);

         if (list == null)
         {
            list = new LinkedList<MessageReference>();

            queueMap.put(queue, list);
         }

         if (ref.cancel(storageManager, postOffice, queueSettingsRepository))
         {
            list.add(ref);
         }
      }

      for (Map.Entry<Queue, LinkedList<MessageReference>> entry : queueMap
              .entrySet())
      {
         LinkedList<MessageReference> refs = entry.getValue();

         entry.getKey().addListFirst(refs);
      }

      clear();

      state = State.ROLLEDBACK;
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

   public void markAsRollbackOnly(MessagingException messagingException)
   {
      state = State.ROLLBACK_ONLY;

      this.messagingException = messagingException;
   }

   public void replay(List<ServerMessage> messages, List<ServerMessage> acknowledgements, State prepared) throws Exception
   {
      containsPersistent = true;
      //acknowledgements.add
      for (ServerMessage message : messages)
      {
         List<MessageReference> refs = postOffice.route(message);
         refsToAdd.addAll(refs);
      }
      for (ServerMessage message : acknowledgements)
      {
          List<MessageReference> refs = postOffice.route(message);
         this.acknowledgements.addAll(refs);
      }
      state = prepared;

   }

   public void setContainsPersistent(final boolean containsPersistent)
   {
      this.containsPersistent = containsPersistent;
   }

   // Private
   // -------------------------------------------------------------------

   private void route(final ServerMessage message) throws Exception
   {
      // We only set the messageID after we are sure the message is not being paged
      // Paged messages won't have an ID until they are depaged
      if (message.getMessageID() == 0l)
      {
         message.setMessageID(storageManager.generateMessageID());
      }

      List<MessageReference> refs = postOffice.route(message);

      refsToAdd.addAll(refs);

      if (message.getDurableRefCount() != 0)
      {
         storageManager.storeMessageTransactional(id, message);

         containsPersistent = true;
      }
   }

   private void pageMessages() throws Exception
   {
      HashSet<SimpleString> pagedDestinationsToSync = new HashSet<SimpleString>();

      boolean pagingPersistent = false;

      if (pagedMessages.size() != 0)
      {
         if (pageTransaction == null)
         {
            pageTransaction = new PageTransactionInfoImpl(this.id);
            // To avoid a race condition where depage happens before the transaction is completed, we need to inform the pager about this transaction is being processed
            pagingManager.addTransaction(pageTransaction);
         }
      }


      for (ServerMessage message: pagedMessages)
      {

         // http://wiki.jboss.org/auth/wiki/JBossMessaging2Paging
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
