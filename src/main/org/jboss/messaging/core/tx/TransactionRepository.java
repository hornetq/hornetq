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
package org.jboss.messaging.core.tx;

import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;


/**
 * 
 * This class maintains JMS Server local transactions
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision 1.1 $
 */
public class TransactionRepository
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionRepository.class);

   // Attributes ----------------------------------------------------
   
   protected Map globalToLocalMap;
   
   protected int txIdSequence;
   
   protected PersistenceManager pm;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRepository(PersistenceManager pm)
   {
      globalToLocalMap = new ConcurrentReaderHashMap();
      this.pm = pm;
   }
   
   // Public --------------------------------------------------------
   
   public Transaction getPreparedTx(Xid xid) throws TransactionException
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
   
   public Transaction createTransaction(Xid xid) throws TransactionException
   {
      if (globalToLocalMap.containsKey(xid))
      {
         throw new TransactionException("There is already a local tx for global tx " + xid);
      }
      Transaction tx = new Transaction(generateTxId(), xid, pm);
      
      if (log.isTraceEnabled()) { log.trace("created transaction " + tx); }
      
      globalToLocalMap.put(xid, tx);
      return tx;
   }
   
   public Transaction createTransaction() throws TransactionException
   {
      Transaction tx = new Transaction(generateTxId(), null, pm);

      if (log.isTraceEnabled()) { log.trace("created transaction " + tx); }

      return tx;
   }
   
   public void setPersistenceManager(PersistenceManager pm)
   {
      this.pm = pm;
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected synchronized int generateTxId()
   {
      return txIdSequence++;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}