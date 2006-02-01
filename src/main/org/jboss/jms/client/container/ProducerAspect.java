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

import javax.jms.Destination;
import javax.jms.Message;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

/**
 * 
 * Handles sending of messages plus handles get and set methods for Producer - 
 * returning state from local cache
 * 
 * This aspect is PER_VM.
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

      String messageID = generateMessageID();
      m.setJMSMessageID(messageID);
      
      ProducerState theState = getState(mi);

      if (deliveryMode == -1)
      {
         //Use the delivery mode of the producer
         deliveryMode = theState.getDeliveryMode();

         if (trace) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
      }
      m.setJMSDeliveryMode(deliveryMode);

      if (priority == -1)
      {
         //Use the priority of the producer
         priority = theState.getPriority();

         if (trace) { log.trace("Using producer's default priority: " + priority); }
      }
      m.setJMSPriority(priority);

      if (theState.isDisableMessageTimestamp())
      {
         m.setJMSTimestamp(0l);
      }
      else
      {
         m.setJMSTimestamp(System.currentTimeMillis());
      }

      if (timeToLive == Long.MIN_VALUE)
      {
         //Use time to live value from producer
         timeToLive = theState.getTimeToLive();

         if (trace) { log.trace("Using producer's default timeToLive: " + timeToLive); }
      }

      if (timeToLive == 0)
      {
         //Zero implies never expires
         m.setJMSExpiration(0);
      }
      else
      {
         m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }

      if (destination == null)
      {
         //Use destination from producer
         destination = theState.getDestination();
         
         if (destination == null)
         {
            throw new UnsupportedOperationException("Destination not specified");
         }

         if (trace) { log.trace("Using producer's default destination: " + destination); }
      }
      else
      {
         //If a default destination was already specified then this must be same destination as that
         //specified in the arguments
         //Destination defaultDestination = state.destination;
         
         if (theState.getDestination() != null && !theState.getDestination().equals(destination))
         {
            throw new UnsupportedOperationException("Where a default destination is specified for the sender " +
                                                    "and a destination is specified in the arguments to the " +
                                                    "send, these destinations must be equal");               
         }
      }

      m.setJMSDestination(destination);
      
      JBossMessage toSend = null;
      if (!(m instanceof MessageDelegate))
      {
         //It's a foreign message
         
         // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
         // a message whose implementation is not one of its own.
         
         toSend = new JBossMessage(m);
      }
      else
      {
         //Get the actual message
         MessageDelegate del = (MessageDelegate)m;
         toSend = del.getMessage();
         toSend.doAfterSend();
         del.setSent();
      }
              
      // We now invoke the send(Message) method - we don't want to pass the rest of the arguments
      // across the wire
      ((ProducerDelegate)invocation.getTargetObject()).send(toSend);
      
      return null;
   }
   
   public Object handleSetDisableMessageID(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setDisableMessageID(((Boolean)args[0]).booleanValue());   
      
      return null;
   }
   
   public Object handleGetDisableMessageID(Invocation invocation) throws Throwable
   {
      return getState(invocation).isDisableMessageID() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleSetDisableMessageTimestamp(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setDisableMessageTimestamp(((Boolean)args[0]).booleanValue());   
      
      return null;
   }
   
   public Object handleGetDisableMessageTimestamp(Invocation invocation) throws Throwable
   {
      return getState(invocation).isDisableMessageTimestamp() ? Boolean.TRUE : Boolean.FALSE;   
   }
   
   public Object handleSetDeliveryMode(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setDeliveryMode(((Integer)args[0]).intValue());          
      
      return null;
   }
   
   public Object handleGetDeliveryMode(Invocation invocation) throws Throwable
   { 
      return new Integer(getState(invocation).getDeliveryMode());  
   }
   
   public Object handleSetPriority(Invocation invocation) throws Throwable
   { 
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setPriority(((Integer)args[0]).intValue());      
      
      return null;
   }
   
   public Object handleGetPriority(Invocation invocation) throws Throwable
   { 
      return new Integer(getState(invocation).getPriority());  
   }
   
   public Object handleSetTimeToLive(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setTimeToLive(((Long)args[0]).longValue());         
      
      return null;
   }
   
   public Object handleGetTimeToLive(Invocation invocation) throws Throwable
   {
      return new Long(getState(invocation).getTimeToLive()); 
   }
   
   public Object handleGetDestination(Invocation invocation) throws Throwable
   {
      return getState(invocation).getDestination();
   }
   
   public Object handleSetDestination(Invocation invocation) throws Throwable
   {
      Object[] args = ((MethodInvocation)invocation).getArguments();
      
      getState(invocation).setDestination((Destination)args[0]);
      
      return null;
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      
      sb.append(new GUID().toString());
      
      return sb.toString();
   }
   
   private ProducerState getState(Invocation inv)
   {
      return (ProducerState)((DelegateSupport)inv.getTargetObject()).getState();
   }
   
   // Inner Classes -------------------------------------------------
   
}

