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
import org.jboss.jms.tx.AckInfo;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;

/**
 * This aspect handles JMS session related logic
 * 
 * This aspect is PER_VM
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
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

   public Object handleClose(Invocation invocation) throws Throwable
   {      
      Object res = invocation.invokeNext();
      
      SessionState state = getState(invocation);
      
      //We must explicitly shutdown the executor

      state.getExecutor().shutdownNow();
         
      return res;
   }      
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      if (ackMode == Session.CLIENT_ACKNOWLEDGE || ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         
         //We store the ack in a list for later acknowledgement or recovery
    
         Object[] args = mi.getArguments();
         
         MessageProxy mp = (MessageProxy)args[0];
         
         int consumerID = ((Integer)args[1]).intValue();
         
         AckInfo info = new AckInfo(mp, consumerID);
         
         state.getToAck().add(info);
         
         if (trace) { log.trace("ack mode is " + Util.acknowledgmentModeToString(ackMode)+ ", acknowledged on " + del); }
      }

      return invocation.invokeNext();
   }
   
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
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         
         //We acknowledge immediately
         
         if (!state.isRecoverCalled())
         {
            //We don't acknowledge the message if recover() was called
            
            //Object[] args = mi.getArguments();
            
            //MessageProxy proxy = (MessageProxy)args[0];
                   
            //int consumerID = ((Integer)args[1]).intValue();

            //AckInfo ack = new AckInfo(proxy, consumerID);
            
            List acks = state.getToAck();
            
            AckInfo ack = (AckInfo)acks.get(0);
            
            del.acknowledge(ack);   
               
            state.getToAck().clear();            
         }
         else
         {
            state.setRecoverCalled(false);
         }
         if (trace) { log.trace("ack mode is " + Util.acknowledgmentModeToString(ackMode)+ ", acknowledged on " + del); }
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
         AckInfo info = (AckInfo)toRedeliver.get(i);
         MessageProxy proxy = info.getMessage();
         proxy.setJMSRedelivered(true);
         
         //TODO delivery count although optional should be global so we need to send it back to the
         //     server but this has performance hit so perhaps we just don't support it?
         proxy.incDeliveryCount();
         
         MessageCallbackHandler handler = state.getCallbackHandler(info.getConsumerID());
              
         if (handler == null)
         {
            // This is ok. The original consumer has closed, this message wil get cancelled back
            // to the channel.
            toCancel.addFirst(info);
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

