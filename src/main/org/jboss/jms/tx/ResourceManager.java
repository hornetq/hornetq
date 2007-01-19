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
package org.jboss.jms.tx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.jms.util.MessagingTransactionRolledBackException;
import org.jboss.jms.util.MessagingXAException;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * The ResourceManager manages work done in both local and global (XA) transactions.
 * 
 * This is one instance of ResourceManager per JMS server. The ResourceManager instances are managed
 * by ResourceManagerFactory.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:Konda.Madhu@uk.mizuho-sc.com">Madhu Konda</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @author <a href="mailto:Cojonudo14@hotmail.com">Hiram Chirino</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ResourceManager
{
   // Constants ------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private ConcurrentHashMap transactions = new ConcurrentHashMap();
   
   // Static ---------------------------------------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ResourceManager.class);
   
   // Constructors ---------------------------------------------------------------------------------
   
   ResourceManager()
   {      
   }
    
   // Public ---------------------------------------------------------------------------------------
   
   /*
    * Merge another resource manager into this one - used in failover
    */
   public void merge(ResourceManager other)
   {
      transactions.putAll(other.transactions);
   }
   
   /**
    * Remove a tx
    */
   public ClientTransaction removeTx(Object xid)
   {
      return removeTxInternal(xid);
   }
            
   /**
    * Create a local tx.
    */
   public LocalTx createLocalTx()
   {
      ClientTransaction tx = new ClientTransaction();
      
      LocalTx xid = getNextTxId();
      
      transactions.put(xid, tx);
      
      return xid;
   }
   
   /**
    * Add a message to a transaction
    * 
    * @param xid - The id of the transaction to add the message to
    * @param m The message
    */
   public void addMessage(Object xid, int sessionId, JBossMessage m)
   {
      if (trace) { log.trace("addding message for xid " + xid); }
      
      ClientTransaction tx = getTxInternal(xid);
      
      tx.addMessage(sessionId, m);
   }
   
   /*
    * Failover session from old session ID -> new session ID
    */
   public void handleFailover(int newServerID, int oldSessionID, int newSessionID)
   {
      for(Iterator i = this.transactions.values().iterator(); i.hasNext(); )
      {
         ClientTransaction tx = (ClientTransaction)i.next();
         tx.handleFailover(newServerID, oldSessionID, newSessionID);
      }                
   }   
   
   /*
    * Get all the deliveries corresponding to the session ID
    */
   public List getDeliveriesForSession(int sessionID)
   {
      List ackInfos = new ArrayList();

      for(Iterator i = transactions.values().iterator(); i.hasNext(); )
      {
         ClientTransaction tx = (ClientTransaction)i.next();
         List acks = tx.getDeliveriesForSession(sessionID);
         
         ackInfos.addAll(acks);
      }

      return ackInfos;
   }
   
   
   /**
    * Add an acknowledgement to the transaction
    * 
    * @param xid - The id of the transaction to add the message to
    * @param ackInfo Information describing the acknowledgement
    */
   public void addAck(Object xid, int sessionId, DeliveryInfo ackInfo) throws JMSException
   {
      if (trace) { log.trace("adding " + ackInfo + " to transaction " + xid); }
      
      ClientTransaction tx = getTxInternal(xid);
      
      if (tx == null)
      {
         throw new JMSException("There is no transaction with id " + xid);
      }
      
      tx.addAck(sessionId, ackInfo);
   }
         
   public void commitLocal(LocalTx xid, ConnectionDelegate connection) throws JMSException
   {
      if (trace) { log.trace("committing " + xid); }
      
      ClientTransaction tx = this.getTxInternal(xid);
      
      // Invalid xid
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find transaction " + xid);
      }
      
      TransactionRequest request =
         new TransactionRequest(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, null, tx);
      
      try
      {
         connection.sendTransaction(request);
         
         // If we get this far we can remove the transaction
         
         this.removeTxInternal(xid);
      }
      catch (Throwable t)
      {
         // If a problem occurs during commit processing the session should be rolled back
         rollbackLocal(xid, connection);
         
         JMSException e = new MessagingTransactionRolledBackException(t.getMessage());
         e.initCause(t);
         throw e;         
      }
   }
   
   public void rollbackLocal(LocalTx xid, ConnectionDelegate connection) throws JMSException
   {
      if (trace) { log.trace("rolling back local xid " + xid); }
      
      ClientTransaction ts = removeTxInternal(xid);
      
      if (ts == null)
      {      
         throw new IllegalStateException("Cannot find transaction with xid:" + xid);         
      }
      
      // don't need messages for rollback
      // We don't clear the acks since we need to redeliver locally
      ts.clearMessages();
      
      // for one phase rollback there is nothing to do on the server
      
      redeliverMessages(ts);
   }
   
   //Only used for testing
   public ClientTransaction getTx(Object xid)
   {
      return getTxInternal(xid);
   }
   
   //Only used for testing
   public int size()
   {
      return transactions.size();
   }
      
   
   public boolean checkForAcksInSession(int sessionId)
   {         
      Iterator iter = transactions.entrySet().iterator();
      
      while (iter.hasNext())
      {         
         Map.Entry entry = (Map.Entry)iter.next();
      
         ClientTransaction tx = (ClientTransaction)entry.getValue();
                  
         if (tx.getState() == ClientTransaction.TX_PREPARED)
         {            
            List dels = tx.getDeliveriesForSession(sessionId);
            
            if (!dels.isEmpty())
            {
               // There are outstanding prepared acks in this session
               
               return true;
            }
         }
      }
      return false;
   }
    
   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------
   
   void commit(Xid xid, boolean onePhase, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("commiting xid " + xid + ", onePhase=" + onePhase); }
      
      ClientTransaction tx = removeTxInternal(xid);
      
      if (trace) { log.trace("Got tx: " + tx + " state " + tx.getState()); }
          
      if (onePhase)
      {
         //Invalid xid
         if (tx == null)
         {       
            throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
         }
         
         TransactionRequest request =
            new TransactionRequest(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, null, tx);
         
         request.state = tx;    
         
         sendTransactionXA(request, connection);
      }
      else
      {
         if (tx != null)
         {
            if (tx.getState() != ClientTransaction.TX_PREPARED)
            {    
               throw new MessagingXAException(XAException.XAER_PROTO, "commit called for transaction, but it is not prepared");
            }
         }
         else
         {
            //It's possible we don't actually have the prepared tx here locally - this
            //may happen if we have recovered from failure and the transaction manager
            //is calling commit on the transaction as part of the recovery process.
         }
         
         TransactionRequest request =
            new TransactionRequest(TransactionRequest.TWO_PHASE_COMMIT_REQUEST, xid, null);
         
         request.xid = xid;      
         
         sendTransactionXA(request, connection);        
      }
      
      if (tx != null)
      {
         tx.setState(ClientTransaction.TX_COMMITED);
      }
   }
      
   void rollback(Xid xid, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("rolling back xid " + xid); }
      
      ClientTransaction tx = removeTxInternal(xid);
      
      //It's possible we don't actually have the prepared tx here locally - this
      //may happen if we have recovered from failure and the transaction manager
      //is calling rollback on the transaction as part of the recovery process.
      
      TransactionRequest request = null;
      
      //don't need the messages
      if (tx != null)
      {
         tx.clearMessages();
      }
             
      if ((tx == null) || tx.getState() == ClientTransaction.TX_PREPARED)
      {
         //2PC rollback
         
         request = new TransactionRequest(TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST, xid, tx);
         
         if (trace) { log.trace("Sending rollback to server, tx:" + tx); }
                                  
         sendTransactionXA(request, connection);
      } 
      else
      {
         //For one phase rollback there is nothing to do on the server 
         
         if (tx == null)
         {     
            throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
         }
      }
                  
      //we redeliver the messages
      //locally to their original consumers if they are still open or cancel them to the server
      //if the original consumers have closed
      
      if (trace) { log.trace("Redelivering messages, tx:" + tx); }
      
      try
      {
         if (tx != null)
         {
            redeliverMessages(tx);
            
            tx.setState(ClientTransaction.TX_ROLLEDBACK);  
         }
         
      }
      catch (JMSException e)
      {
         log.error("Failed to redeliver", e);
      }                               
   }
   
   void endTx(Xid xid, boolean success) throws XAException
   {
      if (trace) { log.trace("ending " + xid + ", success=" + success); }
      
      ClientTransaction state = getTxInternal(xid);
      
      if (state == null)
      {  
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }        
      
      state.setState(ClientTransaction.TX_ENDED);
   }
   
   Xid joinTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("joining  " + xid); }
      
      ClientTransaction state = getTxInternal(xid);
      
      if (state == null)
      {         
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      } 
      
      return xid;
   }
   
   int prepare(Xid xid, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("preparing " + xid); }
      
      ClientTransaction state = getTxInternal(xid);
      
      if (state == null)
      { 
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      } 
      
      TransactionRequest request =
         new TransactionRequest(TransactionRequest.TWO_PHASE_PREPARE_REQUEST, xid, state);
      
      sendTransactionXA(request, connection);      
      
      state.setState(ClientTransaction.TX_PREPARED);
      
      if (trace) { log.trace("State is now: " + state.getState()); }
      
      return XAResource.XA_OK;
   }
   
   Xid resumeTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("resuming " + xid); }
      
      ClientTransaction state = getTxInternal(xid);
      
      if (state == null)
      {       
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }
      
      return xid;
   }
   
   Xid suspendTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("suspending " + xid); }

      ClientTransaction state = getTxInternal(xid);
      
      if (state == null)
      {       
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }
      
      return xid;
   }

   Xid convertTx(LocalTx anonXid, Xid xid) throws XAException
   {
      if (trace) { log.trace("converting " + anonXid + " to " + xid); }

      ClientTransaction state = getTxInternal(anonXid);

      if (state == null)
      {        
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + anonXid);
      }

      state = getTxInternal(xid);

      if (state != null)
      {        
         throw new MessagingXAException(XAException.XAER_DUPID, "Transaction already exists:" + xid);
      }

      ClientTransaction s = removeTxInternal(anonXid);
      
      transactions.put(xid, s);
      
      return xid;
   }
   
   Xid startTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("starting " + xid); }

      ClientTransaction state = getTxInternal(xid);
      
      if (state != null)
      {
         throw new MessagingXAException(XAException.XAER_DUPID, "Transaction already exists with xid " + xid);
      }
            
      transactions.put(xid, new ClientTransaction());
      
      return xid;
   }
   
   Xid[] recover(int flags, ConnectionDelegate conn) throws XAException
   {
      if (trace) { log.trace("calling recover with flags: " + flags); }
      
      if (flags == XAResource.TMSTARTRSCAN)
      {
         try
         {
            Xid[] txs = conn.getPreparedTransactions();

            //populate with TxState --MK
            for (int i = 0; i < txs.length;i++)
            {
               //Don't overwrite if it is already there
               if (!transactions.containsKey(txs[i]))
               {
                  ClientTransaction tx = new ClientTransaction();
   
                  tx.setState(ClientTransaction.TX_PREPARED);
   
                  transactions.put(txs[i], tx);
               }
            }

            return txs;
         }
         catch (JMSException e)
         {
            throw new MessagingXAException(XAException.XAER_RMFAIL, "Failed to get prepared transactions");
         }
      }
      else
      {
         return new Xid[0];
      }
   }
   
   // Private --------------------------------------------------------------------------------------
   
   private ClientTransaction getTxInternal(Object xid)
   {
      if (trace) { log.trace("getting transaction for " + xid); }
      
      return (ClientTransaction)transactions.get(xid);
   }
   
   private ClientTransaction removeTxInternal(Object xid)
   {
      return (ClientTransaction)transactions.remove(xid);
   }
   
   /*
    * Rollback has occurred so we need to redeliver any unacked messages corresponding to the acks
    * is in the transaction.
    * 
    */
   private void redeliverMessages(ClientTransaction ts) throws JMSException
   {
      List sessionStates = ts.getSessionStates();
      
      //Need to do this in reverse order
      
      Collections.reverse(sessionStates);
      
      for (Iterator i = sessionStates.iterator(); i.hasNext();)
      {
         SessionTxState state = (SessionTxState)i.next();
         
         List acks = state.getAcks();
         
         if (!acks.isEmpty())
         {
            DeliveryInfo info = (DeliveryInfo)acks.get(0);
            
            MessageProxy mp = info.getMessageProxy();
            
            SessionDelegate del = mp.getSessionDelegate();
            
            del.redeliver(acks);
         }
      }
   }
   
   private synchronized LocalTx getNextTxId()
   {
      return new LocalTx();
   }
     
   private void sendTransactionXA(TransactionRequest request, ConnectionDelegate connection)
      throws XAException
   {
      try
      {
         connection.sendTransaction(request);
      }
      catch (Throwable t)
      {
         //Catch anything else
         
         //We assume that any error is recoverable - and the recovery manager should retry again
         //either after the network connection has been repaired (if that was the problem), or
         //the server has been fixed.
         
         //(In some cases it will not be possible to fix so the user will have to manually resolve the tx)
         
         //Therefore we throw XA_RETRY
         //Note we DO NOT throw XAER_RMFAIL or XAER_RMERR since both if these will cause the Arjuna
         //tx mgr NOT to retry and the transaction will have to be resolve manually.
         
         throw new MessagingXAException(XAException.XA_RETRY, "A Throwable was caught in sending the transaction", t);
      }
   }
   
   // Inner Classes --------------------------------------------------------------------------------
  
}
