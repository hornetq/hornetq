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
import java.util.LinkedList;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;

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
      
      int ackMode = state.getAcknowledgeMode();

      // select eligible acknowledgments
      List acks = new ArrayList();
      List cancels = new ArrayList();
      for(Iterator i = state.getToAck().iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();
         if (ackMode == Session.AUTO_ACKNOWLEDGE ||
             ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            acks.add(new DefaultAck(ack.getMessageProxy().getDeliveryId()));            
         }
         else
         {
            Cancel cancel = new Cancel(ack.getMessageProxy().getDeliveryId(), ack.getMessageProxy().getDeliveryCount());
            cancels.add(cancel);
         }
         i.remove();
      }
      
      // On closing we acknowlege any AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE, since the session
      // might have closed before the onMessage had finished executing.
      // We cancel any client ack or transactional, we do this explicitly so we can pass the updated
      // delivery count information from client to server. We could just do this on the server but
      // we would lose delivery count info.

      if (!acks.isEmpty())
      {
         del.acknowledgeBatch(acks);
      }
      if (!cancels.isEmpty())
      {
         log.info("Calling canceldeliveries: " + cancels.size());
         del.cancelDeliveries(cancels);
      }

      return invocation.invokeNext();
   }


   public Object handleClose(Invocation invocation) throws Throwable
   {      
      Object res = invocation.invokeNext();

      // We must explicitly shutdown the executor

      SessionState state = getState(invocation);
      state.getExecutor().shutdownNow();

      return res;
   }
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      if (ackMode == Session.CLIENT_ACKNOWLEDGE ||
          ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE ||
          state.getCurrentTxId() == null)
      {
         // We collect acknowledgments (and not transact them) for CLIENT, AUTO and DUPS_OK, and
         // also for XA sessions not enlisted in a global transaction.
 
         // We store the ack in a list for later acknowledgement or recovery
    
         Object[] args = mi.getArguments();
         DeliveryInfo info = (DeliveryInfo)args[0];

         state.getToAck().add(info);
         
         if (trace)
         { 
            SessionDelegate del = (SessionDelegate)mi.getTargetObject();            
            log.trace("ack mode is " + Util.acknowledgmentModeToString(ackMode)+ ", acknowledged on " + del);
         }
      }

      return invocation.invokeNext();
   }
   
   /* Used for client acknowledge */
   public Object handleAcknowledgeAll(Invocation invocation) throws Throwable
   {    
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
    
      if (!state.getToAck().isEmpty())
      {                  
         del.acknowledgeBatch(state.getToAck());
      
         state.getToAck().clear();
      }
        
      return null;
   }
   
   public Object handlePostDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      boolean cancel = ((Boolean)mi.getArguments()[0]).booleanValue();
      
      if (cancel && ackMode != Session.AUTO_ACKNOWLEDGE && ackMode != Session.DUPS_OK_ACKNOWLEDGE)
      {
         throw new IllegalStateException("Ack mode must be AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE");
      }
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE ||
          (ackMode != Session.CLIENT_ACKNOWLEDGE && state.getCurrentTxId() == null))
      {
         // We acknowledge immediately on a non-transacted session that does not want to
         // CLIENT_ACKNOWLEDGE, or an XA session not enrolled in a global transaction.

         SessionDelegate sd = (SessionDelegate)mi.getTargetObject();

         if (!state.isRecoverCalled())
         {
            if (trace) { log.trace("acknowledging NON-transactionally"); }
                        
            List acks = state.getToAck();
            
            // Sanity check
            if (acks.size() != 1)
            {
               throw new IllegalStateException("Should only be one entry in list. " +
                                               "There are " + acks.size());
            }
            
            DeliveryInfo ack = (DeliveryInfo)acks.get(0);
            
            if (cancel)
            {
               List cancels = new ArrayList();
               Cancel c = new Cancel(ack.getMessageProxy().getDeliveryId(), ack.getMessageProxy().getDeliveryCount());
               cancels.add(c);
               sd.cancelDeliveries(cancels);
            }
            else
            {
               sd.acknowledge(ack);
            }
            
            //TODO we can optimise this for the auto_ack case (i.e. not store in list and have to clear each time)
            state.getToAck().clear();
         }
         else
         {
            if (trace) { log.trace("recover called, so NOT acknowledging"); }

            state.setRecoverCalled(false);
         }
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
      
      int ackMode = state.getAcknowledgeMode();
         
      if (ackMode == Session.SESSION_TRANSACTED)
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      
      if (trace) { log.trace("recovering the session"); }
       
      //Call redeliver
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      del.redeliver(state.getToAck());
            
      state.getToAck().clear();

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
    */
   public Object handleRedeliver(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("redeliver called"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
            
      // We put the messages back in the front of their appropriate consumer buffers and set
      // JMSRedelivered to true.
      
      List toRedeliver = (List)mi.getArguments()[0];
      LinkedList toCancel = new LinkedList();
      
      // Need to be recovered in reverse order.
      for (int i = toRedeliver.size() - 1; i >= 0; i--)
      {
         DeliveryInfo info = (DeliveryInfo)toRedeliver.get(i);
         MessageProxy proxy = info.getMessageProxy();        
         
         MessageCallbackHandler handler = state.getCallbackHandler(info.getConsumerId());
              
         if (handler == null)
         {
            // This is ok. The original consumer has closed, this message wil get cancelled back
            // to the channel.
            Cancel cancel = new Cancel(info.getMessageProxy().getDeliveryId(), info.getMessageProxy().getDeliveryCount());
            toCancel.addFirst(cancel);
         }
         else
         {
            handler.addToFrontOfBuffer(proxy);
         }                                    
      }
      
      if (!toCancel.isEmpty())
      {
         // Cancel the messages that can't be redelivered locally
         
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         del.cancelDeliveries(toCancel);
      }
            
      return null;  
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
   

   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private SessionState getState(Invocation inv)
   {
      return (SessionState)((DelegateSupport)inv.getTargetObject()).getState();
   }

   // Inner Classes -------------------------------------------------
   
}

