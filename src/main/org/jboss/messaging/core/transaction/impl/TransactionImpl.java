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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.util.SimpleString;

/**
 * A TransactionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 * 
 * TODO - this should be refactored to use transaction operations for adding, acking paging stuff etc
 */
public class TransactionImpl implements Transaction
{
   private List<TransactionOperation> operations;

   private static final Logger log = Logger.getLogger(TransactionImpl.class);
   
   private static final int INITIAL_NUM_PROPERTIES = 10;
      
   private Object[] properties = new Object[INITIAL_NUM_PROPERTIES];
      
   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final PagingManager pagingManager;

   // FIXME: As part of https://jira.jboss.org/jira/browse/JBMESSAGING-1313
   private final List<ServerMessage> pagedMessages = new ArrayList<ServerMessage>();

   private volatile PageTransactionInfo pageTransaction;

   private final Xid xid;

   private final long id;

   private volatile State state = State.ACTIVE;

   private volatile boolean containsPersistent;

   private MessagingException messagingException;

   private final Object timeoutLock = new Object();

   private final long createTime;
   
   //For a transaction used for depaging, we never want to immediately page the refs again
   //private final boolean depage;

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

   }

   public void prepare() throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state == State.ROLLBACK_ONLY)
         {
            //Do nothing
            return;
         }            
         else if (state != State.ACTIVE)
         {
            throw new IllegalStateException("Transaction is in invalid state " + state);
         }

         if (xid == null)
         {
            throw new IllegalStateException("Cannot prepare non XA transaction");
         }
         
         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.beforePrepare(this);
            }
         }

         pageMessages();

         storageManager.prepare(id, xid);

         state = State.PREPARED;

         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.afterPrepare(this);
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
               //Do nothing
               return;
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
         
         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.beforeCommit(this);
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

         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order
         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }

         clear();

         state = State.COMMITTED;

         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.afterCommit(this);
            }
         }                  
      }
   }

   public void rollback() throws Exception
   {
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
         
         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.beforeRollback(this);
            }
         }

         doRollback();

         state = State.ROLLEDBACK;

         if (operations != null)
         {
            for (TransactionOperation operation : operations)
            {
               operation.afterRollback(this);
            }
         }                  
      }
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
   
   public void setState(final State state)
   {
      this.state = state;
   }

   public Xid getXid()
   {
      return xid;
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

   public void setContainsPersistent(final boolean containsPersistent)
   {
      this.containsPersistent = containsPersistent;
   }

   public void addOperation(final TransactionOperation operation)
   {
      checkCreateOperations();

      operations.add(operation);
   }

   public void removeOperation(final TransactionOperation operation)
   {
      checkCreateOperations();

      operations.remove(operation);
   }
   
   public void setPageTransaction(PageTransactionInfo pageTransaction)
   {
      this.pageTransaction = pageTransaction;      
   }
   
   public void addPagingMessage(final ServerMessage message)
   {
      this.pagedMessages.add(message);
   }
   
   public int getOperationsCount()
   {
      return operations.size();
   }

   public void putProperty(final int index, final Object property)
   {
      if (index >= properties.length)
      {
         Object[] newProperties = new Object[index];
         
         System.arraycopy(properties, 0, newProperties, 0, properties.length);
         
         properties = newProperties;
      }
      
      properties[index] = property;      
   }
   
   public Object getProperty(int index)
   {
      return properties[index];
   }
   
   // Private
   // -------------------------------------------------------------------

   private void doRollback() throws Exception
   {
      if (containsPersistent || xid != null)
      {
         storageManager.rollback(id);
      }

      if (state == State.PREPARED && pageTransaction != null)
      {
         pageTransaction.rollback();
      }

      clear();
   }

   private void checkCreateOperations()
   {
      if (operations == null)
      {
         operations = new ArrayList<TransactionOperation>();
      }
   }

   private void pageMessages() throws Exception
   {
      if (!pagedMessages.isEmpty())
      {
         if (pageTransaction == null)
         {
            pageTransaction = new PageTransactionInfoImpl(id);
            // To avoid a race condition where depage happens before the transaction is completed, we need to inform the
            // pager about this transaction is being processed
            pagingManager.addTransaction(pageTransaction);
         }

         boolean pagingPersistent = false;

         HashSet<SimpleString> pagedDestinationsToSync = new HashSet<SimpleString>();

         // We only need to add the dupl id header once per transaction
         boolean first = true;
         for (ServerMessage message : pagedMessages)
         {
            // http://wiki.jboss.org/wiki/JBossMessaging2Paging
            // Explained under Transaction On Paging. (This is the item B)
            if (pagingManager.page(message, id, first))
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
                                  
               //TODO is this correct - don't we lose transactionality here???
               postOffice.route(message, null);
            }
            first = false;
         }

         if (pagingPersistent)
         {
            containsPersistent = true;
            if (!pagedDestinationsToSync.isEmpty())
            {
               pagingManager.sync(pagedDestinationsToSync);
               storageManager.storePageTransaction(id, pageTransaction);
            }
         }
      }
   }

   private void clear()
   {
      pagedMessages.clear();
   }
}
