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

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.logging.Logger;

/**
 * 
 * Handles sending of messages plus handles get and set methods for Producer - 
 * returning state from local cache
 * 
 * This aspect is PER_INSTANCE.
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ProducerAspect
{   
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ProducerAspect.class);
   
   // Attributes ----------------------------------------------------     
   
   private boolean trace = log.isTraceEnabled();
   
   protected ProducerState producerState;
   
   protected ConnectionState connectionState;
   
   protected SessionState sessionState;
   
   // Static --------------------------------------------------------      
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public Object handleSend(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Object[] args = mi.getArguments();
      
      Destination destination = (Destination)args[0];
      Message m = (Message)args[1];
      int deliveryMode = ((Integer)args[2]).intValue();
      int priority = ((Integer)args[3]).intValue();
      long timeToLive = ((Long)args[4]).longValue();

      // configure the message for sending, using attributes stored as metadata

      ProducerState producerState = getProducerState(mi);

      if (deliveryMode == -1)
      {
         //Use the delivery mode of the producer
         deliveryMode = producerState.getDeliveryMode();
         if (trace) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
      }
      m.setJMSDeliveryMode(deliveryMode);

      if (priority == -1)
      {
         //Use the priority of the producer
         priority = producerState.getPriority();
         if (trace) { log.trace("Using producer's default priority: " + priority); }
      }
      m.setJMSPriority(priority);

      if (producerState.isDisableMessageTimestamp())
      {
         m.setJMSTimestamp(0l);
      }
      else
      {
         m.setJMSTimestamp(System.currentTimeMillis());
      }

      if (timeToLive == Long.MIN_VALUE)
      {
         // Use time to live value from producer
         timeToLive = producerState.getTimeToLive();
         if (trace) { log.trace("Using producer's default timeToLive: " + timeToLive); }
      }

      if (timeToLive == 0)
      {
         // Zero implies never expires
         m.setJMSExpiration(0);
      }
      else
      {
         m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }

      if (destination == null)
      {
         // use destination from producer
         destination = producerState.getDestination();
         
         if (destination == null)
         {
            throw new UnsupportedOperationException("Destination not specified");
         }

         if (trace) { log.trace("Using producer's default destination: " + destination); }
      }
      else
      {
         // if a default destination was already specified then this must be same destination as
         // that specified in the arguments

         if (producerState.getDestination() != null &&
             !producerState.getDestination().equals(destination))
         {
            throw new UnsupportedOperationException("Where a default destination is specified " +
                                                    "for the sender and a destination is " +
                                                    "specified in the arguments to the send, " +
                                                    "these destinations must be equal");
         }
      }

      m.setJMSDestination(destination);

      JBossMessage messageToSend;
      boolean foreign = false;

      if (!(m instanceof MessageProxy))
      {
         // it's a foreign message

         foreign = true;
         
         // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
         // a message whose implementation is not one of its own.

         // create a matching JBossMessage Type from JMS Type
         if(m instanceof BytesMessage)
         {
            messageToSend = new JBossBytesMessage((BytesMessage)m,0);
         }
         else if(m instanceof MapMessage)
         {
            messageToSend = new JBossMapMessage((MapMessage)m,0);
         }
         else if(m instanceof ObjectMessage)
         {
            messageToSend = new JBossObjectMessage((ObjectMessage)m,0);
         }
         else if(m instanceof StreamMessage)
         {
            messageToSend = new JBossStreamMessage((StreamMessage)m,0);
         }
         else if(m instanceof TextMessage)
         {
            messageToSend = new JBossTextMessage((TextMessage)m,0);
         }
         else
         {
            messageToSend = new JBossMessage(m, 0);
         }

         messageToSend.doAfterSend();
      }
      else
      {
         // get the actual message
         MessageProxy proxy = (MessageProxy)m;
         messageToSend = proxy.getMessage();
         messageToSend.doAfterSend();
         proxy.setSent();
      }
      
      // set the message ID
      
      ConnectionState connectionState = getConnectionState(invocation);
      
      long id = connectionState.getIdGenerator().getId();
      
      messageToSend.setJMSMessageID(null);
      messageToSend.setMessageId(id);

      // now that we know the messageID, set it also on the foreign message, if is the case
      if (foreign)
      {
         m.setJMSMessageID(messageToSend.getJMSMessageID());
      }
      
      SessionState sessionState = getSessionState(invocation);
              
      // we now invoke the send(Message) method on the session, which will eventually be fielded
      // by connection endpoint
      ((SessionDelegate)sessionState.getDelegate()).send(messageToSend);
      
      return null;
   }
   
   public Object handleSetDisableMessageID(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setDisableMessageID(((Boolean)args[0]).booleanValue());   
      
      return null;
   }
   
   public Object handleGetDisableMessageID(Invocation invocation) throws Throwable
   {
      return getProducerState(invocation).isDisableMessageID() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleSetDisableMessageTimestamp(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setDisableMessageTimestamp(((Boolean)args[0]).booleanValue());   
      
      return null;
   }
   
   public Object handleGetDisableMessageTimestamp(Invocation invocation) throws Throwable
   {
      return getProducerState(invocation).isDisableMessageTimestamp() ? Boolean.TRUE : Boolean.FALSE;   
   }
   
   public Object handleSetDeliveryMode(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setDeliveryMode(((Integer)args[0]).intValue());          
      
      return null;
   }
   
   public Object handleGetDeliveryMode(Invocation invocation) throws Throwable
   { 
      return new Integer(getProducerState(invocation).getDeliveryMode());  
   }
   
   public Object handleSetPriority(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setPriority(((Integer)args[0]).intValue());      
      
      return null;
   }
   
   public Object handleGetPriority(Invocation invocation) throws Throwable
   { 
      return new Integer(getProducerState(invocation).getPriority());  
   }
   
   public Object handleSetTimeToLive(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setTimeToLive(((Long)args[0]).longValue());         
      
      return null;
   }
   
   public Object handleGetTimeToLive(Invocation invocation) throws Throwable
   {
      return new Long(getProducerState(invocation).getTimeToLive()); 
   }
   
   public Object handleGetDestination(Invocation invocation) throws Throwable
   {
      return getProducerState(invocation).getDestination();
   }
   
   public Object handleSetDestination(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getProducerState(invocation).setDestination((Destination)args[0]);
      
      return null;
   }
   
   public Object handleClosing(Invocation invocation) throws Throwable
   {
      return null;
   }
   
   public Object handleClose(Invocation invocation) throws Throwable
   {
      return null;
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private ProducerState getProducerState(Invocation inv)
   {
      if (producerState == null)
      {
         producerState = (ProducerState)((DelegateSupport)inv.getTargetObject()).getState();
      }
      return producerState;
   }
   
   private ConnectionState getConnectionState(Invocation inv)
   {
      if (connectionState == null)
      {
         connectionState = 
            (ConnectionState)((DelegateSupport)inv.getTargetObject()).getState().getParent().getParent();
      }
      return connectionState;
   }
   
   private SessionState getSessionState(Invocation inv)
   {
      if (sessionState == null)
      {
         sessionState = 
            (SessionState)((DelegateSupport)inv.getTargetObject()).getState().getParent();
      }
      return sessionState;
   }
   
   // Inner Classes -------------------------------------------------
   
}

