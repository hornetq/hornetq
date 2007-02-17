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
package org.jboss.jms.client.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TransactionInProgressException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.jms.tx.LocalTx;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;

/**
 * This aspect handles JMS session related logic
 * 
 * This aspect is PER_VM
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com>Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class SessionAspect
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SessionAspect.class);
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public Object handleClosing(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
            
      if (trace) { log.trace("handleClosing()"); }

      //Sanity check
      if (state.isXA())
      {
         if (trace) { log.trace("Session is XA"); }
         
         ConnectionState connState = (ConnectionState)state.getParent();
         
         ResourceManager rm = connState.getResourceManager();
         
         // An XASession should never be closed if there is prepared ack work that has not yet been
         // committed or rolled back. Imagine if messages had been consumed in the session, and
         // prepared but not committed. Then the connection was explicitly closed causing the
         // session to close. Closing the session causes any outstanding delivered but unacked
         // messages to be cancelled to the server which means they would be available for other
         // consumers to consume. If another consumer then consumes them, then recover() is called
         // and the original transaction is committed, then this means the same message has been
         // delivered twice which breaks the once and only once delivery guarantee.
         
         if (rm.checkForAcksInSession(state.getSessionID()))
         {
            throw new IllegalStateException(
               "Attempt to close an XASession when there are still uncommitted acknowledgements!");
         }        
      }
            
      int ackMode = state.getAcknowledgeMode();
  
      //We need to either ack (for auto_ack) or cancel (for client_ack)
      //any deliveries - this is because the message listener might have closed
      //before on message had finished executing
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         //Acknowledge or cancel any outstanding auto ack
         
         DeliveryInfo remainingAutoAck = state.getAutoAckInfo();
         
         if (remainingAutoAck != null)
         {
            if (trace) { log.trace(this + " handleClosing(). Found remaining auto ack. Will ack " + remainingAutoAck); }
            
            ackDelivery(del, remainingAutoAck);            
                        
            if (trace) { log.trace(this + " acked it"); }
            
            state.setAutoAckInfo(null);
         }
      }
      else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // Cancel any oustanding deliveries
         // We cancel any client ack or transactional, we do this explicitly so we can pass the
         // updated delivery count information from client to server. We could just do this on the
         // server but we would lose delivery count info.
                  
         // CLIENT_ACKNOWLEDGE cannot be used with MDBs (i.e. no connection consumer)
         // so is always safe to cancel on this session                  
         
         cancelDeliveries(del, state.getClientAckList());
         
         state.getClientAckList().clear();
      }
      else if (state.isTransacted() && !state.isXA())
      {
         //We need to explicitly cancel any deliveries back to the server
         //from the resource manager, otherwise delivery count won't be updated
         
         ConnectionState connState = (ConnectionState)state.getParent();
         
         ResourceManager rm = connState.getResourceManager();
         
         List dels = rm.getDeliveriesForSession(state.getSessionID());
         
         cancelDeliveries(del, dels);        
      }
            
      
      return invocation.invokeNext();
   }      
   
   public Object handleClose(Invocation invocation) throws Throwable
   {      
      Object res = invocation.invokeNext();
      
      SessionState state = getState(invocation);

      ConnectionState connState = (ConnectionState)state.getParent();

      Object xid = state.getCurrentTxId();

      if (xid != null)
      {
         //Remove transaction from the resource manager
         connState.getResourceManager().removeTx(xid);
      }

      // We must explicitly shutdown the executor

      state.getExecutor().shutdownNow();

      return res;
   }
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      Object[] args = mi.getArguments();
      DeliveryInfo info = (DeliveryInfo)args[0];
      
      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // We collect acknowledgments in the list
         
         if (trace) { log.trace(this + " added to CLIENT_ACKNOWLEDGE list delivery " + info); }
         
         // Sanity check
         if (info.getConnectionConsumerSession() != null)
         {
            throw new IllegalStateException(
               "CLIENT_ACKNOWLEDGE cannot be used with a connection consumer");
         }
                  
         state.getClientAckList().add(info);
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE ||
               ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         // We collect the single acknowledgement in the state. Currently DUPS_OK is treated the
         // same as AUTO_ACKNOWLDGE.
                           
         if (trace) { log.trace(this + " added " + info + " to session state"); }
         
         state.setAutoAckInfo(info);         
      }
      else
      {             
         Object txID = state.getCurrentTxId();
   
         if (txID != null)
         {
            // the session is non-XA and transacted, or XA and enrolled in a global transaction. An
            // XA session that has not been enrolled in a global transaction behaves as a
            // transacted session.
            
            ConnectionState connState = (ConnectionState)state.getParent();
   
            if (trace) { log.trace("sending acknowlegment transactionally, queueing on resource manager"); }
   
            // If the ack is for a delivery that came through via a connection consumer then we use
            // the connectionConsumer session as the session id, otherwise we use this sessions'
            // session ID
            
            ClientSessionDelegate connectionConsumerDelegate =
               (ClientSessionDelegate)info.getConnectionConsumerSession();
            
            int sessionId = connectionConsumerDelegate != null ?
               connectionConsumerDelegate.getID() : state.getSessionID();
            
            connState.getResourceManager().addAck(txID, sessionId, info);
         }        
      }
      
      return null;
   }
   
   public Object handlePostDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         // We auto acknowledge. Currently DUPS_OK is treated the same as AUTO_ACKNOWLDGE.

         SessionDelegate sd = (SessionDelegate)mi.getTargetObject();

         // It is possible that session.recover() is called inside a message listener onMessage
         // method - i.e. between the invocations of preDeliver and postDeliver. In this case we
         // don't want to acknowledge the last delivered messages - since it will be redelivered.
         if (!state.isRecoverCalled())
         {
            DeliveryInfo delivery = state.getAutoAckInfo();
            
            if (delivery == null)
            {
               throw new IllegalStateException("Cannot find delivery to AUTO_ACKNOWLEDGE");
            }
                                 
            if (trace) { log.trace(this + " auto acknowledging delivery " + delivery); }
              
            // We clear the state before so if the acknowledgment fails then we don't get a knock on
            // exception on the next ack since we haven't cleared the state. See
            // http://jira.jboss.org/jira/browse/JBMESSAGING-852
            
            state.setAutoAckInfo(null);
            
            ackDelivery(sd, delivery);            
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            state.setRecoverCalled(false);
         }
      }

      return null;
   }
   
   /**
    * Used for client acknowledge.
    */
   public Object handleAcknowledgeAll(Invocation invocation) throws Throwable
   {    
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();            
    
      if (!state.getClientAckList().isEmpty())
      {                 
         //CLIENT_ACKNOWLEDGE can't be used with a MDB so it is safe to always acknowledge all
         //on this session (rather than the connection consumer session)
         del.acknowledgeDeliveries(state.getClientAckList());
      
         state.getClientAckList().clear();
      }      
        
      return null;
   }
                       
   /*
    * Called when session.recover is called
    */
   public Object handleRecover(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("recover called"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
            
      SessionState state = getState(invocation);
      
      if (state.isTransacted())
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      
      if (trace) { log.trace("recovering the session"); }
       
      //Call redeliver
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      if (state.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
      {
         del.redeliver(state.getClientAckList());
         
         state.getClientAckList().clear();
      }
      else
      {
         //auto_ack or dups_ok
         
         DeliveryInfo info = state.getAutoAckInfo();
         
         //Don't recover if it's already to cancel
         
         if (info != null)
         {
            List redels = new ArrayList();
            
            redels.add(info);
            
            del.redeliver(redels);
            
            state.setAutoAckInfo(null);            
         }
      }            

      state.setRecoverCalled(true);
      
      return null;  
   }
   
   /**
    * Redelivery occurs in two situations:
    *
    * 1) When session.recover() is called (JMS1.1 4.4.11)
    *
    * "A session's recover method is used to stop a session and restart it with its first
    * unacknowledged message. In effect, the session's series of delivered messages is reset to the
    * point after its last acknowledged message."
    *
    * An important note here is that session recovery is LOCAL to the session. Session recovery DOES
    * NOT result in delivered messages being cancelled back to the channel where they can be
    * redelivered - since that may result in them being picked up by another session, which would
    * break the semantics of recovery as described in the spec.
    *
    * 2) When session rollback occurs (JMS1.1 4.4.7). On rollback of a session the spec is clear
    * that session recovery occurs:
    *
    * "If a transaction rollback is done, its produced messages are destroyed and its consumed
    * messages are automatically recovered. For more information on session recovery, see Section
    * 4.4.11 'Message Acknowledgment.'"
    *
    * So on rollback we do session recovery (local redelivery) in the same as if session.recover()
    * was called.
    * 
    * All cancellation at rollback is driven from the client side - we always attempt to redeliver
    * messages to their original consumers if they are still open, or then cancel them to the server
    * if they are not. Cancelling them to the server explicitly allows the delivery count to be updated.
    * 
    * 
    */
   public Object handleRedeliver(Invocation invocation) throws Throwable
   {            
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);            
            
      // We put the messages back in the front of their appropriate consumer buffers
      
      List toRedeliver = (List)mi.getArguments()[0];
       
      if (trace) { log.trace(this + " handleRedeliver() called: " + toRedeliver); }
      
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      // Need to be redelivered in reverse order.
      for (int i = toRedeliver.size() - 1; i >= 0; i--)
      {
         DeliveryInfo info = (DeliveryInfo)toRedeliver.get(i);
         MessageProxy proxy = info.getMessageProxy();        
         
         MessageCallbackHandler handler = state.getCallbackHandler(info.getConsumerId());
              
         if (handler == null)
         {
            // This is ok. The original consumer has closed, so we cancel the message
            
            //FIXME - this needs to be done atomically for all cancels
            
            cancelDelivery(del, info);
         }
         else
         {
            if (trace) { log.trace("Adding proxy back to front of buffer"); }
            
            handler.addToFrontOfBuffer(proxy);
         }                                    
      }
              
      return null;  
   }
   
   public Object handleCommit(Invocation invocation) throws Throwable
   {
      SessionState state = getState(invocation);

      if (!state.isTransacted())
      {
         throw new IllegalStateException("Cannot commit a non-transacted session");
      }

      if (state.isXA())
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }

      ConnectionState connState = (ConnectionState)state.getParent();
      ConnectionDelegate conn = (ConnectionDelegate)connState.getDelegate();
  
      try
      {
         connState.getResourceManager().commitLocal((LocalTx)state.getCurrentTxId(), conn);
      }
      finally
      {
         //Start new local tx
         Object xid = connState.getResourceManager().createLocalTx();

         state.setCurrentTxId(xid);
      }
      
      //TODO on commit we don't want to ACK any messages that have exceeded the max delivery count OR

      return null;
   }

   public Object handleRollback(Invocation invocation) throws Throwable
   {
      SessionState state = getState(invocation);

      if (!state.isTransacted())
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }

      if (state.isXA())
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }
      
      ConnectionState connState = (ConnectionState)state.getParent();
      ResourceManager rm = connState.getResourceManager();
      ConnectionDelegate conn = (ConnectionDelegate)connState.getDelegate();

      try
      {
         rm.rollbackLocal((LocalTx)state.getCurrentTxId(), conn);
      }
      finally
      {
         // start new local tx
         Object xid = rm.createLocalTx();
         state.setCurrentTxId(xid);
      }

      return null;
   }
   
   public Object handleSend(Invocation invocation) throws Throwable
   {
      SessionState state = getState(invocation);
      Object txID = state.getCurrentTxId();

      if (txID != null)
      {
         // the session is non-XA and transacted, or XA and enrolled in a global transaction, so
         // we add the message to a transaction instead of sending it now. An XA session that has
         // not been enrolled in a global transaction behaves as a non-transacted session.

         ConnectionState connState = (ConnectionState)state.getParent();
         MethodInvocation mi = (MethodInvocation)invocation;
         Message m = (Message)mi.getArguments()[0];

         if (trace) { log.trace("sending message " + m + " transactionally, queueing on resource manager txID=" + txID + " sessionID= " + state.getSessionID()); }

         connState.getResourceManager().addMessage(txID, state.getSessionID(), (JBossMessage)m);

         // ... and we don't invoke any further interceptors in the stack
         return null;
      }

      if (trace) { log.trace("sending message NON-transactionally"); }

      return invocation.invokeNext();
   }
   
   public Object handleGetXAResource(Invocation invocation) throws Throwable
   {
      return getState(invocation).getXAResource();
   }
   
   public Object handleGetTransacted(Invocation invocation) throws Throwable
   {
      return getState(invocation).isTransacted() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleGetAcknowledgeMode(Invocation invocation) throws Throwable
   {
      return new Integer(getState(invocation).getAcknowledgeMode());
   }

   public String toString()
   {
      return "SessionAspect[" + Integer.toHexString(hashCode()) + "]";
   }

   

   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private SessionState getState(Invocation inv)
   {
      return (SessionState)((DelegateSupport)inv.getTargetObject()).getState();
   }
   
   private void ackDelivery(SessionDelegate sess, DeliveryInfo delivery) throws Exception
   {
      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
      
      //If the delivery was obtained via a connection consumer we need to ack via that
      //otherwise we just use this session
      
      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
      
      sessionToUse.acknowledgeDelivery(delivery);      
   }
   
   private void cancelDelivery(SessionDelegate sess, DeliveryInfo delivery) throws Exception
   {
      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
      
      //If the delivery was obtained via a connection consumer we need to cancel via that
      //otherwise we just use this session
      
      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
      
      sessionToUse.cancelDelivery(new DefaultCancel(delivery.getDeliveryID(),
                                  delivery.getMessageProxy().getDeliveryCount(), false, false));      
   }
   
   private void cancelDeliveries(SessionDelegate del, List deliveryInfos) throws Exception
   {
      List cancels = new ArrayList();
      
      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();            
         
         DefaultCancel cancel = new DefaultCancel(ack.getMessageProxy().getDeliveryId(),
                                                  ack.getMessageProxy().getDeliveryCount(),
                                                  false, false);
         
         cancels.add(cancel);
      }  
      
      if (!cancels.isEmpty())
      {
         del.cancelDeliveries(cancels);
      }
   }

   // Inner Classes -------------------------------------------------
   
}

