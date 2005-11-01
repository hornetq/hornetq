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

import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.util.ToString;
import org.jboss.logging.Logger;

/**
 * This interceptor handles JMS session related logic
 * 
 * Important! There is one instance of this interceptor per instance of Session
 * and Connection
 *
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 *
 * $Id$
 */
public class SessionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -8567252489464374932L;
   
   private static final Logger log = Logger.getLogger(SessionInterceptor.class);
   
   // Attributes ----------------------------------------------------
   
   protected int ackMode;
   
   protected ArrayList unacked = new ArrayList();

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ----------------------------------
   
   public String getName()
   {
      return "SessionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {      
      if (!(invocation instanceof MethodInvocation))
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();
      
      if (log.isTraceEnabled()) { log.trace("handling " + methodName); }

      if ("createSessionDelegate".equals(methodName))
      {
         SessionDelegate sessionDelegate = (SessionDelegate)invocation.invokeNext();
         int ackMode = ((Integer)mi.getArguments()[1]).intValue();
         sessionDelegate.setAcknowledgeMode(ackMode);
         return sessionDelegate;
      }
      else if ("acknowledgeSession".equals(methodName))
      {
         //Acknowledge all the messages received in this session
         if (log.isTraceEnabled()) { log.trace("acknowledgeSession called"); }
         
         //This only does anything if in client acknowledge mode
         if (ackMode != Session.CLIENT_ACKNOWLEDGE)
         {
            if (log.isTraceEnabled()) { log.trace("nothing to acknowledge, ending the invocation"); }
            return null;
         }                        
         
         if (log.isTraceEnabled()) { log.trace("I have " + unacked.size() + " messages in the session to ack"); }

         Iterator iter = unacked.iterator();
         try
         {
            while (iter.hasNext())
            {
               AckInfo ackInfo = (AckInfo)iter.next();
               getDelegate(mi).acknowledge(ackInfo.messageID, ackInfo.receiverID);
            }
         }
         finally
         {
            unacked.clear();
         }

         if (log.isTraceEnabled()) { log.trace("session acknowledged, ending the invocation"); }
         return null;
         
      }     
      else if ("postDeliver".equals(methodName))
      {     
         String messageID = (String)mi.getArguments()[0];
         String receiverID = (String)mi.getArguments()[1];         
         
         if (log.isTraceEnabled()) { log.trace("Session ack mode is:" + ackMode); }
         
         if (ackMode == Session.SESSION_TRANSACTED)
         {
            if (log.isTraceEnabled()) { log.trace("session is transacted, noop and ending the invocation"); }
            return null;
         }
         else if (ackMode == Session.AUTO_ACKNOWLEDGE)
         {
            //Just acknowledge now
            if (log.isTraceEnabled()) log.trace("AUTO_ACKNOWLEDGE, so acknowledging to the server");
            getDelegate(mi).acknowledge(messageID, receiverID);
            if (log.isTraceEnabled()) { log.trace("acknowledged, ending the invocation"); }
         }
         else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            //TODO Lazy acks - for now we ack individually
            if (log.isTraceEnabled()) log.trace("DUPS_OK_ACKNOWLEDGE, so lazy acking message");
            getDelegate(mi).acknowledge(messageID, receiverID);
            if (log.isTraceEnabled()) { log.trace("acknowledged, ending the invocation"); }
         }
         else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
         {
            if (log.isTraceEnabled()) log.trace("CLIENT_ACKNOWLEDGE, so storing in unacked msgs");
            unacked.add(new AckInfo(messageID, receiverID));
            if (log.isTraceEnabled()) { log.trace("there are now " + unacked.size() + " unacked messages"); }
         }

         return null;
      }
      else if ("close".equals(methodName))
      {
         if (mi.getMethod().getDeclaringClass().equals(SessionDelegate.class))
         {
            //SessionState state = getSessionState(mi);
            unacked.clear();
         }                        
      }
      else if ("recover".equals(methodName))
      {
         //SessionState state = getSessionState(mi);
         if (log.isTraceEnabled()) { log.trace("recover called"); }
         if (this.ackMode == Session.SESSION_TRANSACTED)
         {
            throw new IllegalStateException("Cannot recover a transacted session");
         }
         unacked.clear();
         
         //Tell the server to redeliver any un-acked messages
         if (log.isTraceEnabled()) { log.trace("redelivering messages"); }
         SessionDelegate del = getDelegate(mi);
         String asfReceiverID = del.getAsfReceiverID();
         
         if (log.isTraceEnabled()) { log.trace("Calling sessiondelegate.redeliver()"); }
         del.cancelDeliveries(asfReceiverID);
         return null;         
      }
      else if ("getAcknowledgeMode".equals(methodName))
      {
         //SessionState state = getSessionState(mi);
         return new Integer(ackMode);
      }
      
      else if ("setAcknowledgeMode".equals(methodName))
      {
         this.ackMode = ((Integer)mi.getArguments()[0]).intValue();
         if (log.isTraceEnabled()) { log.trace("set acknowledgment mode to " + ToString.acknowledgmentMode(ackMode) + ", ending the invocation"); }
         return null;
      }
     
      
      return invocation.invokeNext();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
   
   private SessionDelegate getDelegate(Invocation invocation)
   {
      return (SessionDelegate)getHandler(invocation).getDelegate();
   }
   
   
   
   // Inner Classes -------------------------------------------------
   
	
}

