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

import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.jms.tx.AckInfo;
import org.jboss.logging.Logger;

/**
 * This aspect handles JMS session related logic
 * 
 * This aspect is PER_INSTANCE
 *
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 *
 * $Id$
 */
public class SessionAspect
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SessionAspect.class);
   
   // Attributes ----------------------------------------------------
   
   protected ArrayList unacked = new ArrayList();
   
   protected SessionState state;
      
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleAcknowledgeSession(Invocation invocation) throws Throwable
   {
      //Acknowledge all the messages received in this session
      if (log.isTraceEnabled()) { log.trace("acknowledgeSession called"); }
      
      //This only does anything if in client acknowledge mode
      if (getState(invocation).getAcknowledgeMode() != Session.CLIENT_ACKNOWLEDGE)
      {
         if (log.isTraceEnabled()) { log.trace("nothing to acknowledge, ending the invocation"); }
         
         return null;
      }                        
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (log.isTraceEnabled()) { log.trace("I have " + unacked.size() + " messages in the session to ack"); }

      Iterator iter = unacked.iterator();
      try
      {
         while (iter.hasNext())
         {
            AckInfo ackInfo = (AckInfo)iter.next();
            
            SessionDelegate del = (SessionDelegate)mi.getTargetObject();
            
            del.acknowledge(ackInfo.messageID, ackInfo.receiverID);
         }
      }
      finally
      {
         unacked.clear();
      }

      if (log.isTraceEnabled()) { log.trace("session acknowledged, ending the invocation"); }
      
      return null;
   }
   
   public Object handlePostDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      
      String messageID = (String)mi.getArguments()[0];
      String receiverID = (String)mi.getArguments()[1];         
      
      int ackMode = getState(invocation).getAcknowledgeMode();
      
      if (log.isTraceEnabled()) { log.trace("Session ack mode is:" + ackMode); }
      
      if (ackMode == Session.SESSION_TRANSACTED)
      {
         if (log.isTraceEnabled()) { log.trace("session is transacted, noop and ending the invocation"); }
         
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE)
      {
         //Just acknowledge now
         if (log.isTraceEnabled()) log.trace("AUTO_ACKNOWLEDGE, so acknowledging to the server");
         
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         
         del.acknowledge(messageID, receiverID);
         
         if (log.isTraceEnabled()) { log.trace("acknowledged, ending the invocation"); }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         //TODO Lazy acks - for now we ack individually
         if (log.isTraceEnabled()) log.trace("DUPS_OK_ACKNOWLEDGE, so lazy acking message");
         
         SessionDelegate del = (SessionDelegate)mi.getTargetObject();
         
         del.acknowledge(messageID, receiverID);
         
         if (log.isTraceEnabled()) { log.trace("acknowledged, ending the invocation"); }
      }
      
      return null;
   }
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      
      String messageID = (String)mi.getArguments()[0];
      String receiverID = (String)mi.getArguments()[1];   
      
      int ackMode = getState(invocation).getAcknowledgeMode();
   
      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         if (log.isTraceEnabled()) log.trace("CLIENT_ACKNOWLEDGE, so storing in unacked msgs");
         
         unacked.add(new AckInfo(messageID, receiverID));
         
         if (log.isTraceEnabled()) { log.trace("there are now " + unacked.size() + " unacked messages"); }
      }
      
      if (getState(invocation).isTransacted())
      {
         return invocation.invokeNext();
      }
      else
      {
         return null;
      }
   }
   
   public Object handleClosing(Invocation invocation) throws Throwable
   { 
      unacked.clear();
      
      return invocation.invokeNext();
   }
   
   public Object handleRecover(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("recover called"); }
      
      int ackMode = getState(invocation).getAcknowledgeMode();
      
      if (ackMode == Session.SESSION_TRANSACTED)
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      unacked.clear();
      
      //Tell the server to redeliver any un-acked messages
      if (log.isTraceEnabled()) { log.trace("redelivering messages"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      String asfReceiverID = getState(invocation).getAsfReceiverID();
      
      if (log.isTraceEnabled()) { log.trace("Calling sessiondelegate.redeliver()"); }
      
      del.cancelDeliveries(asfReceiverID);
      
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
      if (state == null)
      {
         state = (SessionState)inv.getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.LOCAL_STATE);
      }
      return state;
   }
    
   // Inner Classes -------------------------------------------------
   
}

