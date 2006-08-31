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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.util.MessagingXAException;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * The ResourceManager manages work done in both local and global (XA) transactions.
 * 
 * This is one instance of ResourceManager per JMS server. The ResourceManager instances are managed
 * by ResourceManagerFactory.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:Cojonudo14@hotmail.com">Hiram Chirino</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ResourceManager
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   protected ConcurrentHashMap transactions = new ConcurrentHashMap();
   
   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ResourceManager.class);
   
   // Constructors --------------------------------------------------
   
   protected ResourceManager()
   {      
   }
   
   // Public --------------------------------------------------------
   
   public TxState getTx(Object xid)
   {
      if (trace) { log.trace("getting transaction for " + xid); }
      
      return (TxState)transactions.get(xid);
   }
   
   public TxState removeTx(Object xid)
   {
      return (TxState)transactions.remove(xid);
   }
   
   /**
    * Create a local tx.
    */
   public LocalTx createLocalTx()
   {
      TxState tx = new TxState();
      
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
   public void addMessage(Object xid, Message m)
   {
      if (trace) { log.trace("addding message for xid " + xid); }
      
      TxState tx = getTx(xid);
      
      tx.getMessages().add(m);
   }
   
   /**
    * Add an acknowledgement to the transaction
    * 
    * @param xid - The id of the transaction to add the message to
    * @param ackInfo Information describing the acknowledgement
    * @param sessionState - the session the ack is in - we need this so on rollback we can tell each session
    * to redeliver it's messages
    */
   public void addAck(Object xid, AckInfo ackInfo) throws JMSException
   {
      if (trace) { log.trace("adding " + ackInfo + " to transaction " + xid); }
      
      TxState tx = getTx(xid);
      
      if (tx == null)
      {
         throw new JMSException("There is no transaction with id " + xid);
      }
      
      tx.getAcks().add(ackInfo);
   }
         
   public void commitLocal(LocalTx xid, ConnectionDelegate connection) throws JMSException
   {
      if (trace) { log.trace("commiting local xid " + xid); }
      
      TxState tx = removeTx(xid);
      
      //Invalid xid
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find transaction with xid:" + xid);
      }
      
      TransactionRequest request =
         new TransactionRequest(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, null, tx);
      
      connection.sendTransaction(request);      
   }
   
   public void rollbackLocal(LocalTx xid, ConnectionDelegate connection) throws JMSException
   {
      if (trace) { log.trace("rolling back local xid " + xid); }
      
      TxState ts = removeTx(xid);
      
      if (ts == null)
      {      
         throw new IllegalStateException("Cannot find transaction with xid:" + xid);         
      }
      
      // don't need messages for rollback
      ts.clearMessages();
      
      // for one phase rollback there is nothing to do on the server
      
      redeliverMessages(ts);
   }
   
   public void commit(Xid xid, boolean onePhase, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("commiting xid " + xid + ", onePhase=" + onePhase); }
      
      TxState tx = removeTx(xid);
          
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
            if (tx.getState() != TxState.TX_PREPARED)
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
         tx.setState(TxState.TX_COMMITED);
      }
   }
      
   public void rollback(Xid xid, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("rolling back xid " + xid); }
      
      TxState tx = removeTx(xid);
          
      TransactionRequest request = null;
      
      // we don't need to send the messages to the server on a rollback
      if (tx != null)
      {
         tx.clearMessages();
      }
      
      if ((tx == null) || tx.getState() == TxState.TX_PREPARED)
      {
         request = new TransactionRequest(TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST, xid, tx);
         
         sendTransactionXA(request, connection);
      } 
      else
      {
         if (tx == null)
         {     
            throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
         }
         
         //For one phase rollback there is nothing to do on the server
      }
      
      try
      {
         redeliverMessages(tx);
      }
      catch (JMSException e)
      {
         log.error("Failed to redeliver", e);
      }
   }
   
   
   /*
    * Rollback has occurred so we need to redeliver any unacked messages corresponding to the acks
    * is in the transaction.
    */
   private void redeliverMessages(TxState ts) throws JMSException
   {
      // Sort messages into lists, one for each session. We use a LinkedHashMap since we need to
      // preserve the order of the sessions.

      Map toAck = new LinkedHashMap();

      for(Iterator i = ts.getAcks().iterator(); i.hasNext(); )
      {
         AckInfo ack = (AckInfo)i.next();
         SessionDelegate del = ack.msg.getSessionDelegate();
         
         List acks = (List)toAck.get(del);
         if (acks == null)
         {
            acks = new ArrayList();
            toAck.put(del, acks);
         }
         acks.add(ack);
      }
      
      // Now tell each session to redeliver.
      
      LinkedList l = new LinkedList();
      
      for(Iterator i = toAck.entrySet().iterator(); i.hasNext();)
      {
         // need to reverse the order
         Object entry = i.next();
         l.addFirst(entry);
      }
      
      for(Iterator i = l.iterator(); i.hasNext();)
      {
         Map.Entry entry = (Map.Entry)i.next();
         SessionDelegate sess = (SessionDelegate)entry.getKey();
         List acks = (List)entry.getValue();
         sess.redeliver(acks);
      }  
   }

   // Protected ------------------------------------------------------
   
   protected void endTx(Xid xid, boolean success) throws XAException
   {
      if (trace) { log.trace("ending " + xid + ", success=" + success); }
      
      TxState state = getTx(xid);
      
      if (state == null)
      {  
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }        
      
      state.setState(TxState.TX_ENDED);
   }
   
   protected Xid joinTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("joining  " + xid); }
      
      TxState state = getTx(xid);
      
      if (state == null)
      {         
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      } 
      
      return xid;
   }
   
   protected int prepare(Xid xid, ConnectionDelegate connection) throws XAException
   {
      if (trace) { log.trace("preparing " + xid); }
      
      TxState state = getTx(xid);
      
      if (state == null)
      { 
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      } 
      
      TransactionRequest request =
         new TransactionRequest(TransactionRequest.TWO_PHASE_PREPARE_REQUEST, xid, state);
      
      sendTransactionXA(request, connection);      
      
      state.setState(TxState.TX_PREPARED);
      
      return XAResource.XA_OK;
   }
   
   protected Xid resumeTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("resuming " + xid); }
      
      TxState state = getTx(xid);
      
      if (state == null)
      {       
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }
      
      return xid;
   }
   
   protected Xid suspendTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("suspending " + xid); }

      TxState state = getTx(xid);
      
      if (state == null)
      {       
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + xid);
      }
      
      return xid;
   }

   protected Xid convertTx(LocalTx anonXid, Xid xid) throws XAException
   {
      if (trace) { log.trace("converting " + anonXid + " to " + xid); }

      TxState state = getTx(anonXid);

      if (state == null)
      {        
         throw new MessagingXAException(XAException.XAER_NOTA, "Cannot find transaction with xid:" + anonXid);
      }

      state = getTx(xid);

      if (state != null)
      {        
         throw new MessagingXAException(XAException.XAER_DUPID, "Transaction already exists:" + xid);
      }

      TxState s = removeTx(anonXid);
      
      transactions.put(xid, s);
      
      return xid;
   }
   
   protected Xid startTx(Xid xid) throws XAException
   {
      if (trace) { log.trace("starting " + xid); }

      TxState state = getTx(xid);
      
      if (state != null)
      {
         throw new MessagingXAException(XAException.XAER_DUPID, "Transaction already exists with xid " + xid);
      }
            
      transactions.put(xid, new TxState());
      
      return xid;
   }
   
   protected Xid[] recover(int flags, ConnectionDelegate conn) throws XAException
   {
      if (trace) { log.trace("calling recover with flags: " + flags); }
      
      if (flags == XAResource.TMSTARTRSCAN)
      {
         try
         {
            Xid[] txs = conn.getPreparedTransactions();
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
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
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
      catch (TransactionRolledBackException e)
      {
         throw new MessagingXAException(XAException.XA_RBROLLBACK, "An error occurred in sending transaction and the transaction was rolled back", e);
      }
      catch (Throwable t)
      {
         //Catch anything else
         throw new MessagingXAException(XAException.XAER_RMERR, "A Throwable was caught in sending the transaction", t);
      }
   }
   
   // Inner Classes --------------------------------------------------
  
}
