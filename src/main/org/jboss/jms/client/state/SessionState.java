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
package org.jboss.jms.client.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.Version;
import org.jboss.jms.tx.MessagingXAResource;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * State corresponding to a session. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionState extends HierarchicalStateSupport
{
   private int acknowledgeMode;
   
   private boolean transacted;
   
   private boolean xa;
   
   private MessagingXAResource xaResource;
   
   private Object currentTxId;
   
   // Executor used for executing onMessage methods
   private QueuedExecutor executor;
   
   private boolean recoverCalled;

   // List<AckInfo>
   private List toAck;

   private ConnectionState parent;
   private SessionDelegate delegate;
   
   private Map callbackHandlers;
   
   public SessionState(ConnectionState parent, SessionDelegate delegate,
                       boolean transacted, int ackMode, boolean xa)
   {
      super(parent, (DelegateSupport)delegate);
      children = new HashSet();
      this.acknowledgeMode = ackMode;
      this.transacted = transacted;
      this.xa = xa;

      if (xa)
      {
         // Create an XA resource
         xaResource = new MessagingXAResource(parent.getResourceManager(), this);                            
      }

      // If session is transacted and XA, the currentTxId will be updated when the XAResource will
      // be enrolled with a global transaction.

      if (transacted & !xa)
      {
         // Create a local tx
         currentTxId = parent.getResourceManager().createLocalTx();        
      }

      executor = new QueuedExecutor(new LinkedQueue());
      
      toAck = new ArrayList();

      // TODO could optimise this to use the same map of callbackmanagers (which holds refs
      // to callbackhandlers) in the connection, instead of maintaining another map
      callbackHandlers = new HashMap();
   }


    public void setParent(HierarchicalState parent)
    {
        this.parent = (ConnectionState)parent;
    }
    public HierarchicalState getParent()
    {
        return parent;
    }

    public DelegateSupport getDelegate()
    {
        return (DelegateSupport)delegate;
    }

    public void setDelegate(DelegateSupport delegate)
    {
        this.delegate=(SessionDelegate)delegate;
    }


   /**
    * @return List<AckInfo>
    */
   public List getToAck()
   {
      return toAck;
   }
    
   public int getAcknowledgeMode()
   {
      return acknowledgeMode;
   }
   
   public boolean isTransacted()
   {
      return transacted;
   }
   
   public boolean isXA()
   {
      return xa;
   }
   
   public MessagingXAResource getXAResource()
   {
      return xaResource;
   }
   
   public QueuedExecutor getExecutor()
   {
      return executor;
   }
   
   public Object getCurrentTxId()
   {
      return currentTxId;
   }
   
   public boolean isRecoverCalled()
   {
      return recoverCalled;
   }
   
   public void setCurrentTxId(Object id)
   {
      this.currentTxId = id;
   }
   
   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }
   
   public void setRecoverCalled(boolean recoverCalled)
   {
      this.recoverCalled = recoverCalled;
   }
   
   public MessageCallbackHandler getCallbackHandler(int consumerID)
   {
      return (MessageCallbackHandler)callbackHandlers.get(new Integer(consumerID));
   }
   
   public void addCallbackHandler(MessageCallbackHandler handler)
   {
      callbackHandlers.put(new Integer(handler.getConsumerId()), handler);
   }
   
   public void removeCallbackHandler(MessageCallbackHandler handler)
   {
      callbackHandlers.remove(new Integer(handler.getConsumerId()));
   }
   
   public List getCallbackHandlers()
   {
      return new ArrayList(callbackHandlers.values());
   }

}

