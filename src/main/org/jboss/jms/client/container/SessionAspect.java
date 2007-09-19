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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TransactionInProgressException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.DefaultCancel;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.BytesMessageProxy;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.MapMessageProxy;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.message.ObjectMessageProxy;
import org.jboss.jms.message.StreamMessageProxy;
import org.jboss.jms.message.TextMessageProxy;
import org.jboss.jms.tx.LocalTx;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;

/**
 * This aspect handles JMS session related logic
 * 
 * This aspect is PER_VM
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com>Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@feodorov.com>Ovidiu Feodorov</a>
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
      if (state.isXA() && !isXAAndConsideredNonTransacted(state))
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
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted(state))
      {
         //Acknowledge or cancel any outstanding auto ack
      	
         DeliveryInfo remainingAutoAck = state.getAutoAckInfo();
         
         if (remainingAutoAck != null)
         {
            if (trace) { log.trace(this + " handleClosing(). Found remaining auto ack. Will ack " + remainingAutoAck); }
            
            try
            {
               ackDelivery(del, remainingAutoAck);
               
               if (trace) { log.trace(this + " acked it"); }               
            }
            finally
            {                        
               state.setAutoAckInfo(null);
            }
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         //Ack any remaining deliveries
                          
         if (!state.getClientAckList().isEmpty())
         {               
            try
            {
               acknowledgeDeliveries(del, state.getClientAckList());
            }
            finally
            {            
               state.getClientAckList().clear();
               
               state.setAutoAckInfo(null);
            }
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
      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted(state))
      {
         // We collect the single acknowledgement in the state. 
                           
         if (trace) { log.trace(this + " added " + info + " to session state"); }
         
         state.setAutoAckInfo(info);         
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         if (trace) { log.trace(this + " added to DUPS_OK_ACKNOWLEDGE list delivery " + info); }
         
         state.getClientAckList().add(info);
         
         //Also set here - this would be used for recovery in a message listener
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
            
            String sessionId = connectionConsumerDelegate != null ?
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
      
      SessionDelegate sd = (SessionDelegate)mi.getTargetObject();

      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted(state))
      {
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
              
            // We clear the state in a finally so then we don't get a knock on
            // exception on the next ack since we haven't cleared the state. See
            // http://jira.jboss.org/jira/browse/JBMESSAGING-852

            //This is ok since the message is acked after delivery, then the client
            //could get duplicates anyway
            
            try
            {
               ackDelivery(sd, delivery);
            }
            finally
            {
               state.setAutoAckInfo(null);               
            }
         }         
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            state.setRecoverCalled(false);
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         List acks = state.getClientAckList();
         
         if (!state.isRecoverCalled())
         {
            if (acks.size() >= state.getDupsOKBatchSize())
            {
               // We clear the state in a finally
               // http://jira.jboss.org/jira/browse/JBMESSAGING-852
         
               try
               {
                  acknowledgeDeliveries(sd, acks);
               }
               finally
               {                  
                  acks.clear();
                  state.setAutoAckInfo(null);
               }
            }    
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            state.setRecoverCalled(false);
         }
         state.setAutoAckInfo(null);
                  
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
         acknowledgeDeliveries(del, state.getClientAckList());
      
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
      
      if (state.isTransacted() && !isXAAndConsideredNonTransacted(state))
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      
      if (trace) { log.trace("recovering the session"); }
       
      //Call redeliver
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      int ackMode = state.getAcknowledgeMode();
      
      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         List dels = state.getClientAckList();
         
         state.setClientAckList(new ArrayList());
         
         del.redeliver(dels);

         state.setRecoverCalled(true);
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE || isXAAndConsideredNonTransacted(state))
      {
         DeliveryInfo info = state.getAutoAckInfo();
         
         //Don't recover if it's already to cancel
         
         if (info != null)
         {
            List redels = new ArrayList();
            
            redels.add(info);
            
            del.redeliver(redels);
            
            state.setAutoAckInfo(null);            

            state.setRecoverCalled(true);
         }
      }   
        

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
         
         ClientConsumer handler = state.getCallbackHandler(info.getConsumerId());
         
         if (handler == null)
         {
            // This is ok. The original consumer has closed, so we cancel the message
            
            cancelDelivery(del, info);
         }
         else if (handler.getRedeliveryDelay() != 0)
         {
         	//We have a redelivery delay in action - all delayed redeliveries are handled on the server
         	
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
      try
      {
         rm.rollbackLocal((LocalTx)state.getCurrentTxId());
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

      // If there is no GlobalTransaction we run it as local transacted
      // as discussed at http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577
      // http://jira.jboss.org/jira/browse/JBMESSAGING-946
      // and
      // http://jira.jboss.org/jira/browse/JBMESSAGING-410
      if ((!state.isXA() && state.isTransacted()) || (state.isXA() && !(txID instanceof LocalTx)))
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
   
   public Object handleCreateMessage(Invocation invocation) throws Throwable
   {
      JBossMessage jbm = new JBossMessage(0);
       
      return new MessageProxy(jbm);
   }
   
   public Object handleCreateBytesMessage(Invocation invocation) throws Throwable
   {
      JBossBytesMessage jbm = new JBossBytesMessage(0);
         
      return new BytesMessageProxy(jbm);
   }
   
   public Object handleCreateMapMessage(Invocation invocation) throws Throwable
   {
      JBossMapMessage jbm = new JBossMapMessage(0);
       
      return new MapMessageProxy(jbm);      
   }
   
   public Object handleCreateObjectMessage(Invocation invocation) throws Throwable
   {
      JBossObjectMessage jbm = new JBossObjectMessage(0);
       
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (mi.getArguments() != null && mi.getArguments().length > 0)
      {
         jbm.setObject((Serializable)mi.getArguments()[0]);
      }
      
      return new ObjectMessageProxy(jbm);
   }
   
   public Object handleCreateStreamMessage(Invocation invocation) throws Throwable
   {
      JBossStreamMessage jbm = new JBossStreamMessage(0);
      
      return new StreamMessageProxy(jbm);
   }
   
   public Object handleCreateTextMessage(Invocation invocation) throws Throwable
   {  
      JBossTextMessage jbm = new JBossTextMessage(0);
      
      MethodInvocation mi = (MethodInvocation)invocation;

      if (mi.getArguments() != null && mi.getArguments().length > 0)
      {
         jbm.setText((String)mi.getArguments()[0]);
      }
      
      return new TextMessageProxy(jbm);
   }      
      
   public Object handleSetMessageListener(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("setMessageListener()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      MessageListener listener = (MessageListener)mi.getArguments()[0];
      
      if (listener == null)
      {
         throw new IllegalStateException("Cannot set a null MessageListener on the session");
      }
      
      getState(invocation).setDistinguishedListener(listener);
      
      return null;
   }
   
   public Object handleGetMessageListener(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("getMessageListener()"); }
      
      return getState(invocation).getDistinguishedListener();
   }
   
   public Object handleCreateConnectionConsumer(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("createConnectionConsumer()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      JBossDestination dest = (JBossDestination)mi.getArguments()[0];
      String subscriptionName = (String)mi.getArguments()[1];
      String messageSelector = (String)mi.getArguments()[2];
      ServerSessionPool sessionPool = (ServerSessionPool)mi.getArguments()[3];
      int maxMessages = ((Integer)mi.getArguments()[4]).intValue();
      
      return new JBossConnectionConsumer((ConnectionDelegate)mi.getTargetObject(), dest,
                                         subscriptionName, messageSelector, sessionPool,
                                         maxMessages);
   }
   
   public Object handleAddAsfMessage(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("addAsfMessage()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      // Load the session with a message to be processed during a subsequent call to run()

      MessageProxy m = (MessageProxy)mi.getArguments()[0];
      String theConsumerID = (String)mi.getArguments()[1];
      String queueName = (String)mi.getArguments()[2];
      int maxDeliveries = ((Integer)mi.getArguments()[3]).intValue();
      SessionDelegate connectionConsumerDelegate = ((SessionDelegate)mi.getArguments()[4]);
      boolean shouldAck = ((Boolean)mi.getArguments()[5]).booleanValue();
      
      if (m == null)
      {
         throw new IllegalStateException("Cannot add a null message to the session");
      }

      AsfMessageHolder holder = new AsfMessageHolder();
      holder.msg = m;
      holder.consumerID = theConsumerID;
      holder.queueName = queueName;
      holder.maxDeliveries = maxDeliveries;
      holder.connectionConsumerDelegate = connectionConsumerDelegate;
      holder.shouldAck = shouldAck;
      
      getState(invocation).getASFMessages().add(holder);
      
      return null;
   }

   public Object handleRun(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("run()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
            
      //This is the delegate for the session from the pool
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();

      LinkedList msgs = state.getASFMessages();
      
      while (msgs.size() > 0)
      {
         AsfMessageHolder holder = (AsfMessageHolder)msgs.removeFirst();

         if (trace) { log.trace("sending " + holder.msg + " to the message listener" ); }
         
         ClientConsumer.callOnMessage(del, state.getDistinguishedListener(), holder.consumerID,
                                              holder.queueName, false,
                                              holder.msg, ackMode, holder.maxDeliveries,
                                              holder.connectionConsumerDelegate, holder.shouldAck);                          
      }
      
      return null;
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
   	if (delivery.isShouldAck())
   	{
	      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
	      
	      //If the delivery was obtained via a connection consumer we need to ack via that
	      //otherwise we just use this session
	      
	      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
	      
	      sessionToUse.acknowledgeDelivery(delivery);
   	}
   }
   
   private void cancelDelivery(SessionDelegate sess, DeliveryInfo delivery) throws Exception
   {
   	if (delivery.isShouldAck())
   	{
	      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
	      
	      //If the delivery was obtained via a connection consumer we need to cancel via that
	      //otherwise we just use this session
	      
	      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
	      
	      sessionToUse.cancelDelivery(new DefaultCancel(delivery.getDeliveryID(),
	                                  delivery.getMessageProxy().getDeliveryCount(), false, false));      
   	}
   }
   
   private void cancelDeliveries(SessionDelegate del, List deliveryInfos) throws Exception
   {
      List cancels = new ArrayList();
      
      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();      
         
         if (ack.isShouldAck())
         {         
	         DefaultCancel cancel = new DefaultCancel(ack.getMessageProxy().getDeliveryId(),
	                                                  ack.getMessageProxy().getDeliveryCount(),
	                                                  false, false);
	         
	         cancels.add(cancel);
         }
      }  
      
      if (!cancels.isEmpty())
      {
         del.cancelDeliveries(cancels);
      }
   }
   
   private void acknowledgeDeliveries(SessionDelegate del, List deliveryInfos) throws Exception
   {
      List acks = new ArrayList();
      
      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();      
         
         if (ack.isShouldAck())
         {         
	         acks.add(ack);
         }
      }  
      
      if (!acks.isEmpty())
      {
         del.acknowledgeDeliveries(acks);
      }
   }

   /** http://jira.jboss.org/jira/browse/JBMESSAGING-946 - To accomodate TCK and the MQ behavior
    *    we should behave as non transacted, AUTO_ACK when there is no transaction enlisted
    *    However when the Session is being used by ASF we should consider the case where
    *    we will convert LocalTX to GlobalTransactions.
    *    This function helper will ensure the condition that needs to be tested on this aspect
    * */
   private boolean isXAAndConsideredNonTransacted(SessionState state)
   {
      return state.isXA() && (state.getCurrentTxId() instanceof LocalTx) && state.getTreatAsNonTransactedWhenNotEnlisted();
   }

   // Inner Classes -------------------------------------------------
   
   private static class AsfMessageHolder
   {
      private MessageProxy msg;
      private String consumerID;
      private String queueName;
      private int maxDeliveries;
      private SessionDelegate connectionConsumerDelegate;
      private boolean shouldAck;
   }
   
}

