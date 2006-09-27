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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;


/**
 * This class maintains JMS Server local transactions.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision 1.1 $
 *
 * $Id$
 */
public class TransactionRepository implements MessagingComponent
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionRepository.class);

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private Map globalToLocalMap;     
   
   private PersistenceManager persistenceManager;
   
   private IdManager idManager;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRepository(PersistenceManager persistenceManager, IdManager idManager)
   {
      this.persistenceManager = persistenceManager;
      
      this.idManager = idManager;
   
      globalToLocalMap = new ConcurrentReaderHashMap();           
   }
   
   // MessagingComponent implementation --------------------------------
   
   public void start() throws Exception
   {
      //NOOP
   }
   
   public void stop() throws Exception
   {
      //NOOP
   }
   
   // Public --------------------------------------------------------   

   public List getPreparedTransactions()
   {
      ArrayList prepared = new ArrayList();
      Iterator iter = globalToLocalMap.values().iterator();
      while (iter.hasNext())
      {
         Transaction tx = (Transaction)iter.next();
         if (tx.xid != null && tx.getState() == Transaction.STATE_PREPARED)
         {
            prepared.add(tx.getXid());
         }
      }
      return prepared;
   }
   
   /*
    * Load any prepared transactions into the repository so they can be recovered
    */
   public void loadPreparedTransactions() throws Exception
   {
      List prepared = null;
            
      prepared = persistenceManager.retrievePreparedTransactions();
      
      if (prepared != null)
      {         
         Iterator iter = prepared.iterator();
         
         while (iter.hasNext())
         {
            Xid xid = (Xid)iter.next();
            Transaction tx = createTransaction(xid);            
            tx.state = Transaction.STATE_PREPARED;
            
            //Load the references for this transaction
         }
      }
   }
         
   public Transaction getPreparedTx(Xid xid) throws Exception
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
   
   public void deleteTransaction(Transaction transaction) throws Exception
   {
	   final Xid id = transaction.getXid();
	   final int state = transaction.getState();
	   
	   if (id==null)
	   {
		   Exception ex = new Exception();
		   log.warn("DeleteTransaction was called for non XA transaction",ex);
		   return;
	   }

	   if (state!=Transaction.STATE_COMMITTED && state!=Transaction.STATE_ROLLEDBACK)
	   {
		   throw new TransactionException("Transaction with xid " + id + " can't be removed as it's not yet commited or rolledback: (Current state is " + Transaction.stateToString(state));
	   }
	   
	   globalToLocalMap.remove(id);
	   
	   
	   
   }
   
   public Transaction createTransaction(Xid xid) throws Exception
   {
      if (globalToLocalMap.containsKey(xid))
      {
         throw new TransactionException("There is already a local tx for global tx " + xid);
      }
      Transaction tx = new Transaction(idManager.getId(), xid, this);
      
      if (trace) { log.trace("created transaction " + tx); }
      
      globalToLocalMap.put(xid, tx);
      
      return tx;
   }
   
   public Transaction createTransaction() throws Exception
   {
      Transaction tx = new Transaction(idManager.getId());

      if (trace) { log.trace("created transaction " + tx); }

      return tx;
   }
   
   public boolean removeTransaction(Xid xid)
   {
      return globalToLocalMap.remove(xid) != null;
   }
   
   /** To be used only by testcases */
   public int getNumberOfRegisteredTransactions()
   {
	  return this.globalToLocalMap.size();   
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------         
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}