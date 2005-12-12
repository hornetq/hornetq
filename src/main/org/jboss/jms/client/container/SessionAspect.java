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

import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.stubs.ClientStubBase;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.logging.Logger;

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
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   
   public Object handlePostDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      
      int ackMode = getState(invocation).getAcknowledgeMode();
      
      if (log.isTraceEnabled()) { log.trace("Session ack mode is:" + ackMode); }
      
      if (ackMode != Session.SESSION_TRANSACTED && ackMode != Session.CLIENT_ACKNOWLEDGE) 
      {
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         
         //We acknowledge immediately
         
         del.acknowledge();                
      }
      
      return null;
   }
   
   
   public Object handleRecover(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("recover called"); }
      
      int ackMode = getState(invocation).getAcknowledgeMode();
      
      if (ackMode == Session.SESSION_TRANSACTED)
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      
      //Tell the server to redeliver any un-acked messages
      if (log.isTraceEnabled()) { log.trace("redelivering messages"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      String asfConsumerID = getState(invocation).getAsfConsumerID();
      
      if (log.isTraceEnabled()) { log.trace("Calling sessiondelegate.redeliver()"); }
      
      del.cancelDeliveries(asfConsumerID);
      
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
      return (SessionState)((ClientStubBase)inv.getTargetObject()).getState();
   }
    
   // Inner Classes -------------------------------------------------
   
}

