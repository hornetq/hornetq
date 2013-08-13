package org.hornetq.tests.integration.ra;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
* Created with IntelliJ IDEA.
* User: andy
* Date: 13/08/13
* Time: 15:13
* To change this template use File | Settings | File Templates.
*/
class DummyTransactionManager implements TransactionManager
{
   protected static DummyTransactionManager tm = new DummyTransactionManager();

   public Transaction tx;

   @Override
   public void begin() throws NotSupportedException, SystemException
   {
   }

   @Override
   public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
   {
   }

   @Override
   public void rollback() throws IllegalStateException, SecurityException, SystemException
   {
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException
   {
   }

   @Override
   public int getStatus() throws SystemException
   {
      return 0;
   }

   @Override
   public Transaction getTransaction() throws SystemException
   {
      return tx;
   }

   @Override
   public void setTransactionTimeout(int i) throws SystemException
   {
   }

   @Override
   public Transaction suspend() throws SystemException
   {
      return null;
   }

   @Override
   public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException
   {
   }
}
