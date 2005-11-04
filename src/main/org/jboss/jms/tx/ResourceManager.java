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

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the ResourceManager used for the XAResources used in
 * JBossMessaging JMS.
 * 
 * Adapted from SpyXAResourceManager
 *
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 * @version $Revision$
 */
public class ResourceManager
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   //TODO - If there are a lot of messages/acks in a tx
   //we need to sensibly deal with this to avoid exhausting memory - 
   //perhaps spill over into a persistent store
   private ConcurrentHashMap transactions = new ConcurrentHashMap();
   
   private long idSequence;
   
   private ConnectionDelegate connection;
   
   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ResourceManager.class);
   
   
   // Constructors --------------------------------------------------
   
   public ResourceManager(ConnectionDelegate connection)
   {
      this.connection = connection;
   }
   
   // Public --------------------------------------------------------
   
   /**
    * Create a local tx. Only used when XAResource is not enlisted as part of global transaction
    */
   public LocalTxXid createLocalTx()
   {
      TxState tx = new TxState();
      LocalTxXid xid = getNextTxId();
      transactions.put(xid, tx);
      return xid;
   }
   
   /**
    * Add a message to a transaction
    * 
    * @param Xid - The id of the transaction to add the message to
    * @param m The message
    */
   public void addMessage(Object xid, Message m)
   {
      if (log.isTraceEnabled()) { log.trace("Addding message for xid:" + xid); }
      TxState tx = getTx(xid);
      tx.messages.add(m);
   }
   
   /**
    * Add an acknowledgement to the transaction
    * 
    * @param Xid - The id of the transaction to add the message to
    * @param ackInfo Information describing the acknowledgement
    */
   public void addAck(Object xid, AckInfo ackInfo) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("Addding ack for xid:" + xid); }
      TxState tx = getTx(xid);
      if (tx == null)
      {
         throw new JMSException("There is no transaction with id:" + xid);
      }
      tx.acks.add(ackInfo);
   }
         
   public void commitLocal(LocalTxXid xid) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("Commiting local xid=" + xid); }
      
      TxState tx = removeTx(xid);
      
      //Invalid xid
      if (tx == null)
      {
         final String msg = "Cannot find transaction with xid:";
         log.error(msg);         
         throw new IllegalStateException(msg);
      }
      
      TransactionRequest request = new TransactionRequest();
      request.requestType = TransactionRequest.ONE_PHASE_COMMIT_REQUEST;
      request.txInfo = tx;
      connection.sendTransaction(request);      
   }
   
   public void rollbackLocal(LocalTxXid xid) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("Rolling back local xid: " + xid); }
      TxState tx = removeTx(xid);
      if (tx == null)
      {
         final String msg = "Cannot find transaction with xid:" + xid;
         log.error(msg);         
         throw new IllegalStateException(msg);         
      }
   }
   
   private void sendTransactionXA(TransactionRequest request) throws XAException
   {
      try
      {
         connection.sendTransaction(request);
      }
      catch (TransactionRolledBackException e)
      {
         log.error("An error occurred in sending transaction and the transaction was rolled back", e);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
      catch (Throwable t)
      {
         //Catch anything else
         log.error("A Throwable was caught in sending the transaction", t);
         throw new XAException(XAException.XAER_RMERR);
      }
   }
   
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Commiting xid=" + xid + ", onePhase=" + onePhase); }
      
      TxState tx = removeTx(xid);
      
      //Invalid xid
      if (tx == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      }
      
      if (onePhase)
      {
         TransactionRequest request = new TransactionRequest();
         request.requestType = TransactionRequest.ONE_PHASE_COMMIT_REQUEST;         
         request.txInfo = tx;    
         sendTransactionXA(request);
      }
      else
      {
         if (tx.state != TxState.TX_PREPARED)
         {
            log.error("commit called for transaction, but it is not prepared");         
            throw new XAException(XAException.XAER_PROTO);
         }
         TransactionRequest request = new TransactionRequest();
         request.xid = xid;
         request.requestType = TransactionRequest.TWO_PHASE_COMMIT_COMMIT_REQUEST;         
         sendTransactionXA(request);
      }
      tx.state = TxState.TX_COMMITED;
   }
   
   
   public void rollback(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Rolling back xid: " + xid); }
      TxState tx = removeTx(xid);
      if (tx == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      }
      if (tx.state == TxState.TX_PREPARED)
      {
         TransactionRequest request = new TransactionRequest();        
         request.requestType = TransactionRequest.TWO_PHASE_COMMIT_ROLLBACK_REQUEST;
         request.xid = xid;
         sendTransactionXA(request);
      }    
   }
   
   public void endTx(Xid xid, boolean success) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Ending xid=" + xid + ", success=" + success); }
      
      TxState state = getTx(xid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      }         
      state.state = TxState.TX_ENDED;
   }
   
   public Xid joinTx(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Joining tx xid=" + xid); }
      
      TxState state = getTx(xid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      } 
      return xid;
   }
   
   public int prepare(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Preparing xid=" + xid); }
      
      TxState state = getTx(xid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      } 
      TransactionRequest request = new TransactionRequest();
      request.requestType = TransactionRequest.TWO_PHASE_COMMIT_PREPARE_REQUEST;
      request.txInfo = state;
      request.xid = xid;
      sendTransactionXA(request);      
      state.state = TxState.TX_PREPARED;
      return XAResource.XA_OK;
   }
   
   public Xid resumeTx(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Resuming tx xid=" + xid); }
      
      TxState state = getTx(xid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      }
      return xid;
   }
   

   public Xid suspendTx(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Suppending tx xid=" + xid); }

      TxState state = getTx(xid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_NOTA);
      }
      return xid;
   }

   public Xid convertTx(LocalTxXid anonXid, Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Converting tx anonXid=" + anonXid + ", xid=" + xid); }

      TxState state = getTx(anonXid);
      if (state == null)
      {
         log.error("Cannot find transaction with xid:" + anonXid);         
         throw new XAException(XAException.XAER_NOTA);
      }
      state = getTx(xid);
      if (state != null)
      {
         log.error("Transaction already exists:" + xid);         
         throw new XAException(XAException.XAER_DUPID);
      }
      TxState s = removeTx(anonXid);

      transactions.put(xid, s);
      return xid;
   }
   
   public Xid startTx(Xid xid) throws XAException
   {
      if (log.isTraceEnabled()) { log.trace("Starting tx xid=" + xid); }

      TxState state = getTx(xid);
      if (state != null)
      {
         log.error("Cannot find transaction with xid:" + xid);         
         throw new XAException(XAException.XAER_DUPID);
      }
      transactions.put(xid, new TxState());
      return xid;
   }
   
   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
   private synchronized LocalTxXid getNextTxId()
   {
      return new LocalTxXid(idSequence++);
   }
   
   public TxState getTx(Object xid)
   {
      if (log.isTraceEnabled()) { log.trace("Getting tx for tx id:" + xid); }
      TxState tx = (TxState) transactions.get(xid);      
      return tx;
   }
   
   public TxState removeTx(Object xid)
   {
      TxState tx = (TxState) transactions.remove(xid);      
      return tx;
   }
   
   // Inner Classes --------------------------------------------------
   
   public static class LocalTxXid
   {
      public long id;
      public LocalTxXid(long l)
      {
         id = l;
      }
      public int hashCode()
      {
         return (int)((id >>> 32) ^ id);
      }
      public boolean equals(Object other)
      {
         if (!(other instanceof LocalTxXid))
         {
            return false;
         }
         LocalTxXid lother = (LocalTxXid)other;
         return lother.id == this.id;
      }
   }  
}
