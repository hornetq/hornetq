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

import javax.jms.IllegalStateException;
import javax.jms.MessageListener;
import javax.jms.ServerSessionPool;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.MessageProxy;
import org.jboss.logging.Logger;


/**
 * An aspect that implements the Application Server Facilities for concurrent processing of 
 * a consumer's messages.
 * 
 * See JMS 1.1 spec. section 8.2
 * 
 * This aspect is PER_INSTANCE.
 *  
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class AsfAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AsfAspect.class);

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   //The list of messages that get processed on a call to run()
   protected LinkedList msgs = new LinkedList();
   
   //The distinguished message listener
   protected MessageListener sessionListener;
   
   protected SessionState state;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleSetMessageListener(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("setMessageListener()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      MessageListener listener = (MessageListener)mi.getArguments()[0];
      
      if (listener == null)
      {
         throw new IllegalStateException("Cannot set a null MessageListener on the session");
      }
      
      sessionListener = listener;
      
      return null;
   }
   
   public Object handleGetMessageListener(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("getMessageListener()"); }
      
      return sessionListener;
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
      int theConsumerID = ((Integer)mi.getArguments()[1]).intValue();
      long channelID = ((Long)mi.getArguments()[2]).longValue();
      int maxDeliveries = ((Integer)mi.getArguments()[3]).intValue();
      
      if (m == null)
      {
         throw new IllegalStateException("Cannot add a null message to the session");
      }

      AsfMessageHolder holder = new AsfMessageHolder();
      holder.msg = m;
      holder.consumerID = theConsumerID;
      holder.channelID = channelID;
      holder.maxDeliveries = maxDeliveries;
      
      msgs.add(holder);

      return null;
   }

   public Object handleRun(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("run()"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
            
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      int ackMode = getSessionState(invocation).getAcknowledgeMode();

      while (msgs.size() > 0)
      {
         AsfMessageHolder holder = (AsfMessageHolder)msgs.removeFirst();

         if (trace) { log.trace("sending " + holder.msg + " to the message listener" ); }
         
         MessageCallbackHandler.callOnMessage(del, sessionListener, holder.consumerID,
                                              holder.channelID, false,
                                              holder.msg, ackMode, holder.maxDeliveries);                          
      }
      
      return null;
   }
  
   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private SessionState getSessionState(Invocation inv)
   {
      return (SessionState)((DelegateSupport)inv.getTargetObject()).getState();
   }

   // Inner Classes --------------------------------------------------
   
   private static class AsfMessageHolder
   {
      private MessageProxy msg;
      private int consumerID;
      private long channelID;
      private int maxDeliveries;
   }
}
