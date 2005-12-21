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

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.SyncSet;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * 
 * State corresponding to a connection.
 * This state is acessible inside aspects/interceptors
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionState extends HierarchicalStateBase
{
   private ResourceManager resourceManager;
   
   //Thread pool used for making asynch calls to server - e.g. activateConsumer
   private PooledExecutor pooledExecutor;
   
   public ConnectionState(ConnectionDelegate delegate, String serverId)
   {
      super(null, delegate);
      children = new SyncSet(new HashSet(), new WriterPreferenceReadWriteLock());
      resourceManager = ResourceManagerFactory.instance.getResourceManager(serverId);
      
      //TODO size should be configurable
      pooledExecutor = new PooledExecutor(20);
   }
    
   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }
   
   public Executor getPooledExecutor()
   {
      return pooledExecutor;
   }

}
