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

import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.TransactionInProgressException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossXAResource;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;
import org.jboss.logging.Logger;

/**
 * This interceptor handles transaction related logic
 * 
 * There should be one instance of this interceptor per instance of Connection, Session
 * and Producer
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 *
 * $Id$
 */
public class TransactionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private final static long serialVersionUID = -34322737839473785L;
   
   private static final Logger log = Logger.getLogger(TransactionInterceptor.class);
   
   // Attributes ----------------------------------------------------
   
   protected ResourceManager rm;
   
   protected JBossXAResource xaResource;
   
   protected boolean transacted;
   
   protected boolean XA;     
   
   //protected Object txID;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Interceptor implementation ------------------------------------
   
   public String getName()
   {
      return "TransactionInterceptor";
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
         SessionDelegate sd = (SessionDelegate)invocation.invokeNext();
         
         boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
         boolean isXA = ((Boolean)mi.getArguments()[2]).booleanValue();
         
         if (log.isTraceEnabled()) { log.trace("setting transacted=" + transacted + ", XA=" + isXA + " on the newly created session"); }
         sd.setTransacted(transacted);
         sd.setXA(isXA);

         if (transacted)
         {            
            ResourceManager theRm = ((ConnectionDelegate)this.getDelegate(mi)).getResourceManager();
            
            if (log.isTraceEnabled()) { log.trace("Transacted so creating XAResource"); }
            
            JBossXAResource theResource = new JBossXAResource(theRm, sd);
                        
            //Create a local tx                     										
            Object Xid = theRm.createLocalTx();
            
            if (log.isTraceEnabled()) { log.trace("Created local tx"); }
            
            theResource.setCurrentTxID(Xid); 
            
            sd.setXAResource(theResource);
            sd.setResourceManager(theRm);
         }            
         return sd;
      }         
      else if ("commit".equals(methodName))
      {
         if (!this.transacted)
         {
            throw new IllegalStateException("Cannot commit a non-transacted session");
         }
         if (this.XA)
         {
            throw new TransactionInProgressException("Cannot call commit on an XA session");
         }
         
         try
         {
            rm.commitLocal((LocalTxXid)xaResource.getCurrentTxID());
         }
         finally
         {
            //Start new local tx
            Object xid = rm.createLocalTx();
            xaResource.setCurrentTxID(xid);
         }
         
         return null;
      }
      else if ("rollback".equals(methodName))
      {
         SessionDelegate sessDelegate = (SessionDelegate)this.getDelegate(mi);
         if (!this.transacted)
         {
            throw new IllegalStateException("Cannot rollback a non-transacted session");
         }
         if (this.XA)
         {
            throw new TransactionInProgressException("Cannot call rollback on an XA session");
         }
         
         try
         {
            rm.rollbackLocal((LocalTxXid)xaResource.getCurrentTxID());
         }
         finally
         {
            //Start new local tx
            Object xid = rm.createLocalTx();
            xaResource.setCurrentTxID(xid);
         } 
         
         //Rollback causes redelivery of messages
         String asfReceiverID = sessDelegate.getAsfReceiverID();
         if (log.isTraceEnabled()) { log.trace("asfReceiverID is:" + asfReceiverID); }
         if (log.isTraceEnabled()) { log.trace("Calling sessiondelegate.redeliver()"); }
         sessDelegate.redeliver(asfReceiverID);
         return null;				
      }
      else if ("send".equals(methodName))
      {
         JMSInvocationHandler handler = getHandler(invocation).getParent();
         
         SessionDelegate sessDelegate = (SessionDelegate)handler.getDelegate();
         
         JBossXAResource jbXAResource = (JBossXAResource)sessDelegate.getXAResource();
         
         ResourceManager theRM = sessDelegate.getResourceManager();
         
         if (log.isTraceEnabled()) { log.trace("XAResource is: " + jbXAResource); }
         
         if (sessDelegate.getTransacted())
         {
            //Session is transacted - so we add message to tx instead of sending now
            
            Object txID = jbXAResource.getCurrentTxID();
            
            if (txID == null)
            {
               boolean isXA = sessDelegate.getXA();
               throw new IllegalStateException("Attempt to send message in tx, but txId is null, XA?" + isXA);
            }
            
            Message m = (Message)mi.getArguments()[1];			
            theRM.addMessage(txID, m);
            
            //And we don't invoke any further interceptors in the stack
            return null;               
         }
         else
         {
            if (log.isTraceEnabled()) { log.trace("Session is not transacted, sending message now"); }
         }
      }
      else if ("preDeliver".equals(methodName))
      {
         //SessionDelegate sessDelegate = (SessionDelegate)this.getDelegate(mi);
         if (!this.transacted)
         {
            //Not transacted - do nothing - ack will happen when delivered() is called
            if (log.isTraceEnabled()) { log.trace("Session is not transacted"); }
            return null;
         }
         else 
         {
            String messageID = (String)mi.getArguments()[0];
            String receiverID = (String)mi.getArguments()[1];
            
            if (log.isTraceEnabled())
            {
               log.trace("XAresource: " + xaResource);
            }
            
            Object txID = xaResource.getCurrentTxID();
            
            if (txID == null)
            {
               throw new IllegalStateException("Attempt to send message in tx, but txId is null, XA?" + this.XA);
            }
            
            rm.addAck(txID, new AckInfo(messageID, receiverID));
            
            return null;
         }
      }  
      else if ("getXAResource".equals(methodName))
      {
         return xaResource;
      }
      else if ("setXAResource".equals(methodName))
      {
         JBossXAResource resource = (JBossXAResource)mi.getArguments()[0];
         this.xaResource = resource;
         return null;
      }
      else if ("getResourceManager".equals(methodName))
      {
         return this.rm;
      }
      else if ("setResourceManager".equals(methodName))
      {
         ResourceManager theRM = (ResourceManager)mi.getArguments()[0];
         this.rm = theRM;
         return null;
      }
      else if ("getTransacted".equals(methodName))
      {
         return new Boolean(transacted);
      }
      else if ("getXA".equals(methodName))
      {
         return new Boolean(XA);
      }
      else if ("setTransacted".equals(methodName))
      {
         this.transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
         if (log.isTraceEnabled()) { log.trace("set transacted to " + transacted + ", ending the invocation"); }
         return null;
      }
      else if ("setXA".equals(methodName))
      {
         this.XA = ((Boolean)mi.getArguments()[0]).booleanValue();
         if (log.isTraceEnabled()) { log.trace("set XA to " + XA + ", ending the invocation"); }
         return null;
      }

      return invocation.invokeNext();            
   }
   
   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
   
   //Helper methods
   
   private Object getDelegate(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler().getDelegate();
   }
    
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
      
   // Inner Classes --------------------------------------------------
   
}


