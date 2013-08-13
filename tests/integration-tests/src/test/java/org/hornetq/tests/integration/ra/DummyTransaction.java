package org.hornetq.tests.integration.ra;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

/**
* Created with IntelliJ IDEA.
* User: andy
* Date: 13/08/13
* Time: 15:13
* To change this template use File | Settings | File Templates.
*/
class DummyTransaction implements Transaction
{
   @Override
   public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException
   {
   }

   @Override
   public void rollback() throws IllegalStateException, SystemException
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
   public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException, SystemException
   {
      return false;
   }

   @Override
   public boolean delistResource(XAResource xaResource, int i) throws IllegalStateException, SystemException
   {
      return false;
   }

   @Override
   public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException
   {
   }
}
