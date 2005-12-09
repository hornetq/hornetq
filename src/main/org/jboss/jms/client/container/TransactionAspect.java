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
import javax.jms.Message;
import javax.jms.TransactionInProgressException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;
import org.jboss.logging.Logger;

/**
 * This aspect handles transaction related logic
 * 
 * This aspect is PER_INSTANCE
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 *
 * $Id$
 */
public class TransactionAspect
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(TransactionAspect.class);
   
   // Attributes ----------------------------------------------------
       
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public Object handleCommit(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
      
      if (!state.isTransacted())
      {
         throw new IllegalStateException("Cannot commit a non-transacted session");
      }
      
      if (state.isXA())
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }
      
      ConnectionState connState = (ConnectionState)state.getParent();
      
      try
      {            
         connState.getResourceManager().commitLocal((LocalTxXid)state.getXAResource().getCurrentTxID());
      }
      finally
      {
         //Start new local tx
         Object xid = connState.getResourceManager().createLocalTx();
         
         state.getXAResource().setCurrentTxID(xid);
      }
      
      return null;
   }
   
   public Object handleRollback(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
      
      SessionDelegate sessDelegate = (SessionDelegate)state.getDelegate();
               
      if (!state.isTransacted())
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }
      
      if (state.isXA())
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }
      
      ConnectionState connState = (ConnectionState)state.getParent();
      
      try
      {
         connState.getResourceManager().rollbackLocal((LocalTxXid)state.getXAResource().getCurrentTxID());
      }
      finally
      {
         //Start new local tx
         Object xid = connState.getResourceManager().createLocalTx();
         
         state.getXAResource().setCurrentTxID(xid);
      } 
      
      //Rollback causes cancellation of messages
      String asfReceiverID = state.getAsfReceiverID();
      
      if (log.isTraceEnabled()) { log.trace("Calling sessiondelegate.cancelAllDeliveries()"); }
      
      sessDelegate.cancelDeliveries(asfReceiverID);
      
      return null;            
   }
   
   public Object handleSend(Invocation invocation) throws Throwable
   {
      ProducerState state = (ProducerState)getState(invocation);
      
      SessionState sessState = (SessionState)state.getParent();
                        
      if (sessState.isTransacted())
      {
         //Session is transacted - so we add message to tx instead of sending now
         
         Object txID = sessState.getXAResource().getCurrentTxID();
         
         if (txID == null)
         {            
            throw new IllegalStateException("Attempt to send message in tx, but txId is null, XA?" + sessState.isXA());
         }
         
         ConnectionState connState = (ConnectionState)sessState.getParent();
         
         MethodInvocation mi = (MethodInvocation)invocation;
         
         Message m = (Message)mi.getArguments()[1];         
         
         connState.getResourceManager().addMessage(txID, m);
         
         //And we don't invoke any further interceptors in the stack
         return null;               
      }
      else
      {      
         return invocation.invokeNext();
      }
   }
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
      
      if (!state.isTransacted())
      {
         //Not transacted - do nothing - ack will happen when delivered() is called
         if (log.isTraceEnabled()) { log.trace("session is not transacted, this is a noop, returning"); }
         
         return null;
      }
      else 
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         
         String messageID = (String)mi.getArguments()[0];
         
         String receiverID = (String)mi.getArguments()[1];
         
         Object txID = state.getXAResource().getCurrentTxID();
         
         if (txID == null)
         {
            throw new IllegalStateException("Attempt to send message in tx, but txId is null, XA?" + state.isXA());
         }
         
         ConnectionState connState = (ConnectionState)state.getParent();
         
         connState.getResourceManager().addAck(txID, new AckInfo(messageID, receiverID));
         
         return null;
      }
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
   private Object getState(Invocation inv)
   {
      return inv.getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.LOCAL_STATE);      
   }
   
   // Inner Classes --------------------------------------------------
   
}


