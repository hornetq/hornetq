/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.tx;

import java.util.Map;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;


/**
 * 
 * This class maintains JMS Server local transactions
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class TransactionRepository
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionRepository.class);

   // Attributes ----------------------------------------------------
   
   protected Map globalToLocalMap;
   
   protected long txIdSequence;
   
   protected PersistenceManager pm;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRepository(PersistenceManager pm)
   {
      globalToLocalMap = new ConcurrentReaderHashMap();
      this.pm = pm;
   }
   
   // Public --------------------------------------------------------
   
   public Transaction getPreparedTx(Object xid) throws TransactionException
   {
      Transaction tx = (Transaction)globalToLocalMap.get(xid);
      if (tx == null)
      {
         throw new TransactionException("Cannot find local tx for xid:" + xid);
      }
      if (tx.getState() != Transaction.STATE_PREPARED)
      {
         throw new TransactionException("Transaction with xid " + xid + " is not in prepared state");
      }
      return tx;
   }
   
   public Transaction createTransaction(Object xid) throws TransactionException
   {
      if (globalToLocalMap.containsKey(xid))
      {
         throw new TransactionException("There is already a local tx for global tx " + xid);
      }
      Transaction tx = createTransaction();
      globalToLocalMap.put(xid, tx);
      return tx;
   }
   
   public Transaction createTransaction() throws TransactionException
   {
      Transaction tx = new Transaction(generateTxId(), pm);

      if (log.isTraceEnabled()) { log.trace("created transaction " + tx); }

      return tx;
   }
   
   public void setPersistenceManager(PersistenceManager pm)
   {
      this.pm = pm;
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected synchronized long generateTxId()
   {
      return txIdSequence++;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}