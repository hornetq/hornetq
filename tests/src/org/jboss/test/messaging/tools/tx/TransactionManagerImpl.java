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
package org.jboss.test.messaging.tools.tx;

import org.jboss.messaging.util.NotYetImplementedException;

import javax.transaction.TransactionManager;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.RollbackException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.Transaction;
import javax.transaction.InvalidTransactionException;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

/**
 * A transaction manager implementation used for testing.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TransactionManagerImpl implements TransactionManager
{
   // Constants -----------------------------------------------------

   public static final String BROKEN = "BROKEN";
   public static final String OPERATIONAL = "OPERATIONAL";

   // Static --------------------------------------------------------

   private static TransactionManagerImpl singleton = new TransactionManagerImpl();

   public static TransactionManagerImpl getInstance()
   {
      return singleton;
   }

   // Attributes ----------------------------------------------------

   // True if the TransactionManager should keep a <GlobalId - transaction> map
   private boolean globalIdsEnabled = false;

   private ThreadLocal threadLocal = new ThreadLocal();

   private long timeout = 5 * 60 * 1000;

   // Active transaction map <LocalID - TransactionImpl>
   private Map localIdTx = Collections.synchronizedMap(new HashMap());

   // Active transaction map <GlobalID - TransactionImpl>
   private Map globalIdTx = Collections.synchronizedMap(new HashMap());


   // A count of the transactions that have been committed
   private volatile int commitCount;

   // A count of the transactions that have been rolled back
   private volatile int rollbackCount;

   private RecoveryLogger recovery;

   private String state;

   // Constructors --------------------------------------------------

   private TransactionManagerImpl()
   {

   }

   // TransactionManager implementation -----------------------------

   public void begin() throws NotSupportedException, SystemException
   {
      ThreadInfo ti = getThreadInfo();
      TransactionImpl current = ti.transaction;

      if (current != null)
      {
         if (current.isDone())
         {
            disassociateThread(ti);
         }
         else
         {
            String msg = "Transaction already active, cannot nest transactions.";
            throw new NotSupportedException(msg);
         }
      }

      long timeout = (ti.timeout == 0) ? this.timeout : ti.timeout;
      TransactionImpl transaction = new TransactionImpl(timeout);
      associateThread(ti, transaction);
      localIdTx.put(transaction.getLocalId(), transaction);
      if (globalIdsEnabled)
      {
         globalIdTx.put(transaction.getGlobalId(), transaction);
      }

   }


   public void commit() throws RollbackException, HeuristicMixedException,
                               HeuristicRollbackException, SecurityException,
                               IllegalStateException, SystemException
   {
      if (BROKEN.equals(state))
      {
         throw new SystemException("THIS IS AN EXCEPTION THAT SIMULATES A PROBLEM " +
                                   "WITH THE TRANSACTION MANAGER");
      }

      ThreadInfo ti = getThreadInfo();
      TransactionImpl current = ti.transaction;

      if (current != null)
      {
         current.commit();
         disassociateThread(ti);
      }
      else
      {
         throw new IllegalStateException("No transaction.");
      }
   }


   public void rollback() throws IllegalStateException, SecurityException, SystemException
   {
      ThreadInfo ti = getThreadInfo();
      TransactionImpl current = ti.transaction;

      if (current != null)
      {
         if (!current.isDone())
         {
            current.rollback();
            return;
         }
         disassociateThread(ti);
      }
      throw new IllegalStateException("No transaction.");
   }


   public void setRollbackOnly() throws IllegalStateException, SystemException
   {
      throw new NotYetImplementedException();
   }


   public int getStatus() throws SystemException
   {
      throw new NotYetImplementedException();
   }


   public Transaction getTransaction() throws SystemException
   {

      if (BROKEN.equals(state))
      {
         throw new SystemException("THIS IS AN EXCEPTION THAT SIMULATES A PROBLEM " +
                                   "WITH THE TRANSACTION MANAGER");
      }

      ThreadInfo ti = getThreadInfo();
      TransactionImpl current = ti.transaction;

      if (current != null && current.isDone())
      {
         current = null;
         disassociateThread(ti);
      }

      return current;
   }


   public void setTransactionTimeout(int seconds) throws SystemException
   {
      throw new NotYetImplementedException();
   }


   public Transaction suspend() throws SystemException
   {
      throw new NotYetImplementedException();
   }


   public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException,
                                               SystemException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   /**
    * Used for testing.
    */
   public void setState(String state)
   {

      if (BROKEN.equals(state) ||
          OPERATIONAL.equals(state))
      {
         this.state = state;
      }
      else
      {
         throw new IllegalArgumentException("Unknon state: " + state);
      }
   }

   // Package protected ---------------------------------------------

   void setRecovery(RecoveryLogger recovery)
   {
      this.recovery = recovery;
   }

   RecoveryLogger getRecovery()
   {
      return recovery;
   }

   long getCommitCount()
   {
      return commitCount;
   }

   long getRollbackCount()
   {
      return rollbackCount;
   }

   void incCommitCount()
   {
      ++commitCount;
   }

   void incRollbackCount()
   {
      ++rollbackCount;
   }

   void releaseTransactionImpl(TransactionImpl transaction)
   {
      localIdTx.remove(transaction.getLocalId());
      if (globalIdsEnabled)
      {
         globalIdTx.remove(transaction.getGlobalId());
      }
   }

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   /**
    * Return the ThreadInfo for the calling thread, and create if not found.
    */
   private ThreadInfo getThreadInfo()
   {
      ThreadInfo ti = (ThreadInfo)threadLocal.get();

      if (ti == null)
      {
         ti = new ThreadInfo();
         ti.timeout = timeout;
         threadLocal.set(ti);
      }
      return ti;
   }

   public Transaction disassociateThread()
   {
      return disassociateThread(getThreadInfo());
   }

   private Transaction disassociateThread(ThreadInfo ti)
   {
      TransactionImpl current = ti.transaction;
      ti.transaction = null;
      current.disassociateCurrentThread();
      return current;
   }

   public void associateThread(Transaction transaction)
   {
      if (transaction != null && !(transaction instanceof TransactionImpl))
      {
         throw new RuntimeException("Not a TransactionImpl, but a " +
                                    transaction.getClass().getName());
      }

      TransactionImpl transactionImpl = (TransactionImpl)transaction;
      ThreadInfo ti = getThreadInfo();
      ti.transaction = transactionImpl;
      transactionImpl.associateCurrentThread();
   }

   private void associateThread(ThreadInfo ti, TransactionImpl transaction)
   {
      ti.transaction = transaction;
      transaction.associateCurrentThread();
   }





   // Inner classes -------------------------------------------------

   static class ThreadInfo
   {
      long timeout;
      TransactionImpl transaction;
   }

}
