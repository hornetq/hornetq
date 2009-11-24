/*
60 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.transaction.impl;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;

/**
 * A TransactionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 * 
 */
public class TransactionImpl implements Transaction
{
   private List<TransactionOperation> operations;

   private static final Logger log = Logger.getLogger(TransactionImpl.class);

   private static final int INITIAL_NUM_PROPERTIES = 10;

   private Object[] properties = new Object[INITIAL_NUM_PROPERTIES];

   private final StorageManager storageManager;

   private final Xid xid;

   private final long id;

   private volatile State state = State.ACTIVE;

   private HornetQException exception;

   private final Object timeoutLock = new Object();

   private final long createTime;

   private volatile boolean containsPersistent;

   public TransactionImpl(final StorageManager storageManager)
   {
      this.storageManager = storageManager;

      xid = null;

      id = storageManager.generateUniqueID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager, final PostOffice postOffice)
   {
      this.storageManager = storageManager;

      this.xid = xid;

      id = storageManager.generateUniqueID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager)
   {
      this.storageManager = storageManager;

      this.xid = xid;

      this.id = id;

      createTime = System.currentTimeMillis();
   }

   // Transaction implementation
   // -----------------------------------------------------------

   public void setContainsPersistent()
   {
      this.containsPersistent = true;
   }

   public long getID()
   {
      return id;
   }

   public long getCreateTime()
   {
      return createTime;
   }

   public void prepare() throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state == State.ROLLBACK_ONLY)
         {
            if (exception != null)
            {
               throw exception;
            }
            else
            {
               // Do nothing
               return;
            }
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

         storageManager.prepare(id, xid);

         state = State.PREPARED;
         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
               log.warn("IO Error completing the transaction, code = " + errorCode + ", message = " + errorMessage);
            }

            public void done()
            {
               if (operations != null)
               {
                  for (TransactionOperation operation : operations)
                  {
                     try
                     {
                        operation.afterPrepare(TransactionImpl.this);
                     }
                     catch (Exception e)
                     {
                        // https://jira.jboss.org/jira/browse/HORNETQ-188
                        // After commit shouldn't throw an exception
                        log.warn(e.getMessage(), e);
                     }
                  }
               }
            }
         });
      }
   }

   public void commit() throws Exception
   {
      commit(true);
   }

   public void commit(boolean onePhase) throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state == State.ROLLBACK_ONLY)
         {
            if (exception != null)
            {
               throw exception;
            }
            else
            {
               // Do nothing
               return;
            }
         }

         if (xid != null)
         {
            if (onePhase && state != State.ACTIVE || !onePhase && state != State.PREPARED)
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

         if (containsPersistent || (xid != null && state == State.PREPARED))
         {
            storageManager.commit(id);

            state = State.COMMITTED;
         }

         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         // This also will only use a different thread if there are any IO pendings.
         // If the IO finished early by the time we got here, we won't need an executor
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
               log.warn("IO Error completing the transaction, code = " + errorCode + ", message = " + errorMessage);
            }

            public void done()
            {
               if (operations != null)
               {
                  for (TransactionOperation operation : operations)
                  {
                     try
                     {
                        operation.afterCommit(TransactionImpl.this);
                     }
                     catch (Exception e)
                     {
                        // https://jira.jboss.org/jira/browse/HORNETQ-188
                        // After commit shouldn't throw an exception
                        log.warn(e.getMessage(), e);
                     }
                  }
               }
            }
         });

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

         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
               log.warn("IO Error completing the transaction, code = " + errorCode + ", message = " + errorMessage);
            }

            public void done()
            {
               if (operations != null)
               {
                  for (TransactionOperation operation : operations)
                  {
                     try
                     {
                        operation.afterRollback(TransactionImpl.this);
                     }
                     catch (Exception e)
                     {
                        // https://jira.jboss.org/jira/browse/HORNETQ-188
                        // After commit shouldn't throw an exception
                        log.warn(e.getMessage(), e);
                     }
                  }
               }
            }
         });
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

   public void markAsRollbackOnly(final HornetQException exception)
   {
      state = State.ROLLBACK_ONLY;

      this.exception = exception;
   }

   public synchronized void addOperation(final TransactionOperation operation)
   {
      checkCreateOperations();

      operations.add(operation);
   }

   public void removeOperation(final TransactionOperation operation)
   {
      checkCreateOperations();

      operations.remove(operation);
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
      if (containsPersistent || (xid != null && state == State.PREPARED))
      {
         storageManager.rollback(id);
      }
   }

   private void checkCreateOperations()
   {
      if (operations == null)
      {
         operations = new ArrayList<TransactionOperation>();
      }
   }

}
