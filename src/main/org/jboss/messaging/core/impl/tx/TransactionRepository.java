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
package org.jboss.messaging.core.impl.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.SimpleDelivery;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains JMS Server local transactions.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:Konda.Madhu@uk.mizuho-sc.com">Madhu Konda</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
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

   protected MessageStore messageStore;

   private IDManager idManager;
   
   private PostOffice postOffice;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRepository(PersistenceManager persistenceManager, MessageStore store, IDManager idManager)
   {
      this.persistenceManager = persistenceManager;
      
      this.messageStore = store;
      
      this.idManager = idManager;
      
      globalToLocalMap = new ConcurrentHashMap();        
   }
   
   // Injection ----------------------------------------
   
   //Unfortunately we have to do this for now, since PostOffice is not started until after this is constructed
   //We will sort out dependencies properly when we go to the micro container
   public void injectPostOffice(PostOffice po)
   {
      postOffice = po;
   }
   

   // MessagingComponent implementation --------------------------------
   
   public void start() throws Exception
   {
      //NOOP
   }
   
   // Public --------------------------------------------------------
      
   public void stop() throws Exception
   {
      //NOOP
   }
   
   // Public --------------------------------------------------------   

   /**
    * Attempts to recover existing prepared transactions by redelivering unhandled messages and acknowledgements
    * on the appropriate channels.
    *
    * @return  List of Xid instances
    */
   public synchronized List recoverPreparedTransactions()
   {
      if (trace) { log.trace(this + " recoverPreparedTransactions()"); }
      
      ArrayList prepared = new ArrayList();

      Iterator iter = globalToLocalMap.values().iterator();
      
      while (iter.hasNext())
      {
         Transaction tx = (Transaction) iter.next();
         
         if (tx.getXid() != null && tx.getState() == Transaction.STATE_PREPARED)
         {
            try
            {
               if (trace) log.trace("Loading and handling refs and acks to the Tx "+tx);

               //The transaction might have been created, prepared, without the server crashing
               //in which case the tx will already have the references and acks in them
               //in this case we DO NOT want to replay them again, since they will end up in the transaction state
               //twice
               //In other words we only want to replay acks and sends if this tx was recovered from the db
               if (tx.isRecoveredFromStorage())
               {
                  tx.loadState();
               }
            }
            catch (Exception e)
            {
               log.warn("Failed to replay transaction (XID: " + tx.getXid() + ", LocalID: " + tx.getId() + ") during recovery.", e);
            }

            prepared.add(tx.getXid());
         }
      }
      
      if (trace) { log.trace("Returning " + prepared.size() + " transactions"); }

      return prepared;
   }

   /*
    * Load any prepared transactions into the repository so they can be
    * recovered
    */
   public void loadPreparedTransactions() throws Exception
   {
      if (trace) log.trace("load prepared transactions...");

      List prepared = null;

      prepared = persistenceManager.retrievePreparedTransactions();

      if (trace) log.trace ("found " + prepared.size() + " transactions in prepared state");

      if (prepared != null)
      {
         Iterator iter = prepared.iterator();

         while (iter.hasNext())
         {
            PreparedTxInfo txInfo = (PreparedTxInfo) iter.next();

            //This method may be called more than once - e.g. when failover occurs so we don't want to add the
            //prepared tx if it is already in memory
            
            if (!globalToLocalMap.containsKey(txInfo.getXid()))
            {
               Transaction tx = createTransaction(txInfo);
               
               tx.setState(Transaction.STATE_PREPARED);
               
               tx.setRecoveredFromStorage(true);
               
               if (trace) log.trace("reinstating TX(XID: " + txInfo.getXid() + ", LocalId " + txInfo.getTxId() +")");
               
            }  
            else
            {
               if (trace) log.trace("Not adding to map since it's already in map");
            }
         }
      }     
   }
   
   public List getPreparedTransactions()
   {
      return new ArrayList(globalToLocalMap.keySet());
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
	   
	   if (id == null)
	   {
		   throw new IllegalArgumentException("DeleteTransaction was called for non XA transaction");
	   }

	   if (state != Transaction.STATE_COMMITTED && state != Transaction.STATE_ROLLEDBACK)
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
      Transaction tx = new Transaction(idManager.getID(), xid, this);
      
      if (trace) { log.trace("created transaction " + tx); }
      
      globalToLocalMap.put(xid, tx);
      
      return tx;
   }
   
   public Transaction createTransaction() throws Exception
   {
      Transaction tx = new Transaction(idManager.getID());
      
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
   
   /**
    * Load the references and invoke the channel to handle those refs
    */
   void handleReferences(Transaction tx) throws Exception
   {
      if (trace) log.trace("Handle references for TX(XID: " + tx.getXid() + ", LocalID: " + tx.getId()+ "):");

      long txId = tx.getId();

      List pairs = persistenceManager.getMessageChannelPairRefsForTx(txId);

      if (trace) log.trace("Found " + pairs.size() + " unhandled references.");

      for (Iterator iter = pairs.iterator(); iter.hasNext();)
      {
         PersistenceManager.MessageChannelPair pair = (PersistenceManager.MessageChannelPair)iter.next();
         
         Message msg = pair.getMessage();
         
         long channelID = pair.getChannelId();
         
         MessageReference ref = null;
         
         try
         {
            ref = messageStore.reference(msg);         

            Binding binding = postOffice.getBindingForChannelID(channelID);
            
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for channel id " + channelID);
            }
            
            Queue queue = binding.queue;
                        
            if (trace) log.trace("Destination for message[ID=" + ref.getMessage().getMessageID() + "] is: " + queue);
   
            // The actual jmx queue may not have been deployed yet, so we need to activate the core queue if so, 
            // or the handle will have no effect
                        
            boolean deactivate = false;

            if (!queue.isActive())
            {
            	queue.activate();

            	deactivate = true;
            }

            queue.handle(null, ref, tx);

            if (deactivate)
            {
            	queue.deactivate();
            }
         }
         finally
         {
            if (ref != null)
            {
               //Need to release reference
               ref.releaseMemoryReference();
            }
         }
      }
   }

   /**
    * Load the acks and acknowledge them
    */
   void handleAcks(Transaction tx) throws Exception
   {
      long txId = tx.getId();
      
      List pairs = persistenceManager.getMessageChannelPairAcksForTx(txId);

      if (trace) log.trace("Found " + pairs.size() + " unhandled acks.");
      
      List dels = new ArrayList();

      for (Iterator iter = pairs.iterator(); iter.hasNext();)
      {
         PersistenceManager.MessageChannelPair pair = (PersistenceManager.MessageChannelPair)iter.next();
         
         Message msg = pair.getMessage();
         
         long channelID = pair.getChannelId();
         
         MessageReference ref = null;
         
         try
         {
            ref = messageStore.reference(msg);         

            Binding binding = postOffice.getBindingForChannelID(channelID);
            
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for channel id " + channelID);
            }                       
   
            Queue queue = binding.queue;
            
            if (trace) log.trace("Destination for message[ID=" + ref.getMessage().getMessageID() + "] is: " + queue);            
            
            //Create a new delivery - note that it must have a delivery observer otherwise acknowledge will fail
            Delivery del = new SimpleDelivery(queue, ref, true, true);

            if (trace) log.trace("Acknowledging..");

            try
            {
            	// The actual jmx queue may not have been deployed yet, so we need to the core queue if so, 
               // or the acknowledge will have no effect
                           
               boolean deactivate = false;

               if (!queue.isActive())
               {
               	queue.activate();

               	deactivate = true;
               }

               del.acknowledge(tx);

               if (deactivate)
               {
               	queue.deactivate();
               }
            }
            catch (Throwable t)
            {
               log.error("Failed to acknowledge " + del + " during recovery", t);
            }
            
            dels.add(del);            
         }
         finally
         {
            if (ref != null)
            {
               //Need to release reference
               ref.releaseMemoryReference();
            }
         }
      }
      
      if (!dels.isEmpty())
      {
         //Add a callback so these dels get cancelled on rollback
         tx.addCallback(new CancelCallback(dels), this);
      }
      
   }
   
   // Protected -----------------------------------------------------         
   
   // Private -------------------------------------------------------

	         
      
	/**
	 * Creates a prepared transaction
	 *
	 * @param txInfo
	 * @return
	 * @throws Exception
	 */
	private Transaction createTransaction(PreparedTxInfo txInfo) throws Exception
   {
		if (globalToLocalMap.containsKey(txInfo.getXid()))
      {
			throw new TransactionException(
					"There is already a local tx for global tx "	+ txInfo.getXid());
		}

		// Resurrected tx
		Transaction tx = new Transaction(txInfo.getTxId(), txInfo.getXid(), this);

		if (trace) {
			log.trace("created transaction " + tx);
		}

		globalToLocalMap.put(txInfo.getXid(), tx);

		return tx;
	}

   // Inner classes -------------------------------------------------
   
   private class CancelCallback implements TxCallback
   {
      private List toCancel;
      
      private CancelCallback(List toCancel)
      {
         this.toCancel = toCancel;
      }

      public void afterCommit(boolean onePhase) throws Exception
      {
      }

      public void afterPrepare() throws Exception
      {
      }

      public void afterRollback(boolean onePhase) throws Exception
      {
         //On rollback we need to cancel the ref back into the channel
         //We only need to do this if the tx was reloaded since otherwise the
         //cancel will come from the SCE
         
         //Need to cancel in reverse
         
         for (int i = toCancel.size() - 1; i >= 0; i--)
         {                     
            Delivery del = (Delivery)toCancel.get(i);
            
            try
            {
               del.cancel();
            }
            catch (Throwable t)
            {
               log.error("Failed to cancel delivery", t);
               throw new TransactionException(t.getMessage(), t);
            }
         }
      }

      public void beforeCommit(boolean onePhase) throws Exception
      {
      }

      public void beforePrepare() throws Exception
      {
      }

      public void beforeRollback(boolean onePhase) throws Exception
      {
      }
      
   }
   
}