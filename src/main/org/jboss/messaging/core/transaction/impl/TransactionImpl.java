/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;

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

   private final List<MessageReference> refsToAdd = new ArrayList<MessageReference>();

   private final List<MessageReference> acknowledgements = new ArrayList<MessageReference>();

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

      this.xid = null;

      this.id = storageManager.generateTransactionID();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager,
                          final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.xid = xid;

      this.id = storageManager.generateTransactionID();
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

      List<MessageReference> refs = postOffice.route(message);

      refsToAdd.addAll(refs);

      if (message.getDurableRefCount() != 0)
      {
         storageManager.storeMessageTransactional(id, message);

         containsPersistent = true;
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
               message.decrementDurableRefCount();

               if (message.getDurableRefCount() == 0)
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

      if (containsPersistent)
      {
         storageManager.prepare(id);
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

      if (containsPersistent)
      {
         storageManager.commit(id);
      }

      for (MessageReference ref : refsToAdd)
      {
         ref.getQueue().addLast(ref);
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

         if (message.isDurable() && queue.isDurable())
         {
            // Reverse the decrements we did in the tx
            message.incrementDurableRefCount();
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

   public void setContainsPersistent(final boolean containsPersistent)
   {
      this.containsPersistent = containsPersistent;
   }

   // Private
   // -------------------------------------------------------------------

   private void clear()
   {
      refsToAdd.clear();

      acknowledgements.clear();
   }
}
