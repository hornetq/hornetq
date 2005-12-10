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

import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ServerSessionPool;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.stubs.ClientStubBase;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.logging.Logger;
import org.jgroups.protocols.JMS;

/**
 * An aspect that implements the Application Server Facilities for concurrent processing of 
 * a consumer's messages.
 * 
 * @see JMS 1.1 spec. section 8.2
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
      if (log.isTraceEnabled()) { log.trace("setMessageListener"); }
      
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
      if (log.isTraceEnabled()) { log.trace("getMessageListener"); }
      
      return sessionListener;
   }
   
   public Object handleCreateConnectionConsumer(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("createConnectionConsumer"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination dest = (Destination)mi.getArguments()[0];
      String subscriptionName = (String)mi.getArguments()[1];
      String messageSelector = (String)mi.getArguments()[2];
      ServerSessionPool sessionPool = (ServerSessionPool)mi.getArguments()[3];
      int maxMessages = ((Integer)mi.getArguments()[4]).intValue();
      
      ConnectionConsumer cc =
         new JBossConnectionConsumer((ConnectionDelegate)mi.getTargetObject(), dest, subscriptionName,
                                     messageSelector, sessionPool, maxMessages);        
      return cc;       
   }
   
   public Object handleAddAsfMessage(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("addAsfMessage"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      //Load the session with a message to be processed during a subsequent call to run()
      
      SessionState theState = getState(invocation);
      
      Message m = (Message)mi.getArguments()[0];
      String theReceiverID = (String)mi.getArguments()[1];
      ConsumerDelegate cons = (ConsumerDelegate)mi.getArguments()[2];
      
      String currReceiverID = theState.getAsfReceiverID();
      
      if (m == null)
      {
         throw new IllegalStateException("Cannot add a null message to the session");
      }
      if (theReceiverID == null)
      {
         throw new IllegalStateException("Cannot add a message without specifying receiverID");
      }
      
      if (currReceiverID != null && currReceiverID != theReceiverID)
      {
         throw new IllegalStateException("Cannot receive messages from more than one receiver");
      }
      theState.setAsfReceiverID(theReceiverID);
      
      AsfMessageHolder holder = new AsfMessageHolder();
      holder.msg = m;
      holder.consumerID = theReceiverID;
      holder.consumerDelegate = cons;
      
      msgs.add(holder);

      return null;
   }

   public Object handleRun(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("run"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
            
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      int ackMode = getState(invocation).getAcknowledgeMode();

      while (msgs.size() > 0)
      {
         AsfMessageHolder holder = (AsfMessageHolder)msgs.removeFirst();
         
         MessageCallbackHandler.callOnMessage(holder.consumerDelegate, del, sessionListener, holder.consumerID, false, holder.msg, ackMode);                          
      }
      
      return null;
   }
  
   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private SessionState getState(Invocation inv)
   {
      return (SessionState)((ClientStubBase)inv.getTargetObject()).getState();
   }
   
   // Inner Classes --------------------------------------------------
   
   protected static class AsfMessageHolder
   {
      Message msg;
      String consumerID;
      ConsumerDelegate consumerDelegate;
   }
}
