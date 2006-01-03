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

import java.util.HashSet;

import org.jboss.jms.client.JBossXAResource;
import org.jboss.jms.delegate.SessionDelegate;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * State corresponding to a session. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionState extends HierarchicalStateBase
{
   private int acknowledgeMode;
   
   private boolean transacted;
   
   private boolean xa;
   
   private JBossXAResource xaResource;
   
   private Object currentTxId;
   
   //Executor used for executing onMessage methods
   private Executor executor;
   
   public SessionState(ConnectionState parent, SessionDelegate delegate,
                       boolean transacted, int ackMode, boolean xa)
   {
      super(parent, delegate);
      children = new HashSet();
      this.acknowledgeMode = ackMode;
      this.transacted = transacted;
      this.xa = xa;      
      if (xa)
      {
         //Create an XA resource
         xaResource = new JBossXAResource(parent.getResourceManager(), this);                            
      }
      if (transacted)
      {
         //Create a local tx                                                  
         currentTxId = parent.getResourceManager().createLocalTx();        
      }
      executor = new QueuedExecutor(new LinkedQueue());
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
   
   public JBossXAResource getXAResource()
   {
      return xaResource;
   }
   
   public Executor getExecutor()
   {
      return executor;
   }
   
   public Object getCurrentTxId()
   {
      return currentTxId;
   }
   
   public void setCurrentTxId(Object id)
   {
      this.currentTxId = id;
   }

}

