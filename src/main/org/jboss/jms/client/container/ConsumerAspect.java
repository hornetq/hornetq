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

import javax.jms.MessageListener;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * Handles operations related to the consumer.
 * 
 * This aspect is PER_VM.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsumerAspect
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;

      ConsumerDelegate consumerDelegate = (ConsumerDelegate)invocation.invokeNext();

      boolean isCC = ((Boolean)mi.getArguments()[4]).booleanValue();

      // Create the message handler
      SessionState sessionState =
         (SessionState)((DelegateSupport)invocation.getTargetObject()).getState();
      ConnectionState connectionState = (ConnectionState)sessionState.getParent();
      SessionDelegate sessionDelegate = (SessionDelegate)invocation.getTargetObject();
      ConsumerState consumerState = (ConsumerState)((DelegateSupport)consumerDelegate).getState();
      int consumerID = consumerState.getConsumerID();
      long channelID = consumerState.getChannelID();
      int prefetchSize = consumerState.getBufferSize();
      QueuedExecutor sessionExecutor = sessionState.getExecutor();
      int maxDeliveries = consumerState.getMaxDeliveries();
      
      MessageCallbackHandler messageHandler =
         new MessageCallbackHandler(isCC, sessionState.getAcknowledgeMode(),
                                    sessionDelegate, consumerDelegate, consumerID,
                                    channelID,
                                    prefetchSize, sessionExecutor, maxDeliveries);
      
      sessionState.addCallbackHandler(messageHandler);
      
      CallbackManager cm = connectionState.getRemotingConnection().getCallbackManager();
      cm.registerHandler(consumerID, messageHandler);
         
      consumerState.setMessageCallbackHandler(messageHandler);
      
      //Now we have finished creating the client consumer, we can tell the SCD
      //we are ready
      consumerDelegate.changeRate(1);

      return consumerDelegate;
   }
   
   public Object handleClosing(Invocation invocation) throws Throwable
   {      
      ConsumerState consumerState = getState(invocation);
        
      
      // First we call close on the messagecallbackhandler which waits for onMessage invocations      
      // to complete any further messages received will be ignored
      consumerState.getMessageCallbackHandler().close();
      
      long lastDeliveryId = consumerState.getMessageCallbackHandler().getLastDeliveryId();
      
      SessionState sessionState = (SessionState)consumerState.getParent();
      ConnectionState connectionState = (ConnectionState)sessionState.getParent();
                 
      sessionState.removeCallbackHandler(consumerState.getMessageCallbackHandler());

      CallbackManager cm = connectionState.getRemotingConnection().getCallbackManager();
      cm.unregisterHandler(consumerState.getConsumerID());
            
      // Then we make sure closing is called on the ServerConsumerEndpoint.

      Object res = invocation.invokeNext();
      
      //Now we send a message to the server consumer with the last delivery id so
      //it can cancel any inflight messages after that
      //This needs to be done *after* the call to closing has been executed on the server
      //maybe it can be combined with closing
      
      ConsumerDelegate del = (ConsumerDelegate)invocation.getTargetObject();
         
      //Now we need to cancel any inflight messages - this must be done before
      //cancelling the message callback handler buffer, so that messages end up back in the channel
      //in the right order
      del.cancelInflightMessages(lastDeliveryId);
      
      //And then we cancel any messages still in the message callback handler buffer     
      consumerState.getMessageCallbackHandler().cancelBuffer();
                                   
      return res;
   }      
   
   public Object handleReceive(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      Object[] args = mi.getArguments();
      long timeout = args == null ? 0 : ((Long)args[0]).longValue();
      
      return getMessageCallbackHandler(invocation).receive(timeout);
   }
   
   public Object handleReceiveNoWait(Invocation invocation) throws Throwable
   {      
      return getMessageCallbackHandler(invocation).receive(-1);
   }
   
   public Object handleSetMessageListener(Invocation invocation) throws Throwable
   {   
      MethodInvocation mi = (MethodInvocation)invocation;
      Object[] args = mi.getArguments();
      MessageListener l = (MessageListener)args[0];
      
      getMessageCallbackHandler(invocation).setMessageListener(l);
      
      return null;
   }
   
   public MessageListener handleGetMessageListener(Invocation invocation) throws Throwable
   {       
      return getMessageCallbackHandler(invocation).getMessageListener();
   }
   
   public Object handleGetDestination(Invocation invocation) throws Throwable
   {
      return getState(invocation).getDestination();
   }
   
   public Object handleGetNoLocal(Invocation invocation) throws Throwable
   {
      return getState(invocation).isNoLocal() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleGetMessageSelector(Invocation invocation) throws Throwable
   {
      return getState(invocation).getSelector();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private ConsumerState getState(Invocation inv)
   {
      return (ConsumerState)((DelegateSupport)inv.getTargetObject()).getState();
   }
   
   private MessageCallbackHandler getMessageCallbackHandler(Invocation inv)
   {      
      ConsumerState state = (ConsumerState)((DelegateSupport)inv.getTargetObject()).getState();
      return state.getMessageCallbackHandler();      
   }
   
   // Inner classes -------------------------------------------------
}
