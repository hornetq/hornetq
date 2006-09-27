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
import javax.jms.Session;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.LocalTx;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;

/**
 * This aspect handles transaction related logic
 * 
 * This aspect is PER_VM.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com>Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class TransactionAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionAspect.class);

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleClose(Invocation invocation) throws Throwable
   {
      Object res = invocation.invokeNext();

      SessionState state = (SessionState)getState(invocation);

      ConnectionState connState = (ConnectionState)state.getParent();

      Object xid = state.getCurrentTxId();

      if (xid != null)
      {
         //Remove transaction from the resource manager
         connState.getResourceManager().removeTx(xid);
      }

      return res;
   }

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
         connState.getResourceManager().commitLocal((LocalTx)state.getCurrentTxId(), conn);
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
      ResourceManager rm = connState.getResourceManager();
      ConnectionDelegate conn = (ConnectionDelegate)connState.getDelegate();

      try
      {
         rm.rollbackLocal((LocalTx)state.getCurrentTxId(), conn);
      }
      finally
      {
         // start new local tx
         Object xid = rm.createLocalTx();
         state.setCurrentTxId(xid);
      }

      return null;
   }

   public Object handleSend(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
      Object txID = state.getCurrentTxId();

      if (txID != null)
      {
         // the session is non-XA and transacted, or XA and enrolled in a global transaction, so
         // we add the message to a transaction instead of sending it now. An XA session that has
         // not been enrolled in a global transaction behaves as a non-transacted session.

         ConnectionState connState = (ConnectionState)state.getParent();
         MethodInvocation mi = (MethodInvocation)invocation;
         Message m = (Message)mi.getArguments()[0];

         if (trace) { log.trace("sending message " + m + " transactionally, queueing on resource manager"); }

         connState.getResourceManager().addMessage(txID, m);

         // ... and we don't invoke any further interceptors in the stack
         return null;
      }

      if (trace) { log.trace("sending message NON-transactionally"); }

      return invocation.invokeNext();
   }

   public Object handlePreDeliver(Invocation invocation) throws Throwable
   {
      SessionState state = (SessionState)getState(invocation);
      Object txID = state.getCurrentTxId();

      if (txID != null)
      {
         // the session is non-XA and transacted, or XA and enrolled in a global transaction. An
         // XA session that has not been enrolled in a global transaction behaves as a
         // non-transacted session.

         MethodInvocation mi = (MethodInvocation)invocation;
         MessageProxy proxy = (MessageProxy)mi.getArguments()[0];
         int consumerID = ((Integer)mi.getArguments()[1]).intValue();
         AckInfo info = new AckInfo(proxy, consumerID, Session.SESSION_TRANSACTED);
         ConnectionState connState = (ConnectionState)state.getParent();

         if (trace) { log.trace("sending acknowlegment transactionally, queueing on resource manager"); }

         connState.getResourceManager().addAck(txID, info);
      }

      return null;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   private HierarchicalState getState(Invocation inv)
   {
      return ((DelegateSupport)inv.getTargetObject()).getState();
   }

   // Inner Classes --------------------------------------------------

}


