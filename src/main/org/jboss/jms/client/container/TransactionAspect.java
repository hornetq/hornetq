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
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.stubs.ClientStubBase;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;

/**
 * This aspect handles transaction related logic
 * 
 * This aspect is PER_VM.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class TransactionAspect
{
   // Constants -----------------------------------------------------
   
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
      ConnectionDelegate conn = (ConnectionDelegate)connState.getDelegate();
      
      try
      {            
         connState.getResourceManager().commitLocal((LocalTxXid)state.getCurrentTxId(), conn);
      }
      finally
      {
         //Start new local tx
         Object xid = connState.getResourceManager().createLocalTx();
         
         state.setCurrentTxId(xid);
      }
      
      return null;
   }
   
   public Object handleRollback(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
           
      if (!state.isTransacted())
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }
      
      if (state.isXA())
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }
      
      ConnectionState connState = (ConnectionState)state.getParent();
      ConnectionDelegate conn = (ConnectionDelegate)connState.getDelegate();
      
      try
      {
         connState.getResourceManager().rollbackLocal((LocalTxXid)state.getCurrentTxId(), conn);
      }
      finally
      {
         //Start new local tx
         Object xid = connState.getResourceManager().createLocalTx();
         
         state.setCurrentTxId(xid);
      } 
      
      return null;            
   }
   
   public Object handleSend(Invocation invocation) throws Throwable
   {
      ProducerState state = (ProducerState)getState(invocation);
      
      SessionState sessState = (SessionState)state.getParent();
                        
      if (sessState.isTransacted())
      {
         //Session is transacted - so we add message to tx instead of sending now
         
         Object txID = sessState.getCurrentTxId();
         
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
      
      if (state.isTransacted())
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         
         String messageID = (String)mi.getArguments()[0];
         
         String consumerID = (String)mi.getArguments()[1];
         
         Object txID = state.getCurrentTxId();
         
         if (txID == null)
         {
            throw new IllegalStateException("Attempt to send message in tx, but txId is null, XA?" + state.isXA());
         }
         
         ConnectionState connState = (ConnectionState)state.getParent();
         
         //Add the acknowledgement to the transaction
         
         connState.getResourceManager().addAck(txID, new AckInfo(messageID, consumerID));                  
      }
      
      return null;
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
   private HierarchicalState getState(Invocation inv)
   {
      return ((ClientStubBase)inv.getTargetObject()).getState();    
   }
   
   // Inner Classes --------------------------------------------------
   
}


