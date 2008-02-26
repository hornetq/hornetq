package org.jboss.messaging.core.persistence.impl.bdbje.integration;

import org.jboss.messaging.core.persistence.impl.bdbje.BDBJETransaction;

import com.sleepycat.je.Transaction;

/**
 * 
 * A RealBDBJETransaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RealBDBJETransaction implements BDBJETransaction
{
   private final Transaction transaction;
   
   RealBDBJETransaction(final Transaction transaction)
   {
      this.transaction = transaction;
   }
      
   public void commit() throws Exception
   {
      transaction.commit();
   }

   public void rollback() throws Exception
   {
      transaction.abort();
   }
   
   public Transaction getTransaction()
   {
      return transaction;
   }

}
