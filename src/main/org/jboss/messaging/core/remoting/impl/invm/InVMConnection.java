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
package org.jboss.messaging.core.remoting.impl.invm;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * A InVMConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMConnection implements Connection
{
   private static final Logger log = Logger.getLogger(InVMConnection.class);
   
   private final BufferHandler handler;
   
   private final ConnectionLifeCycleListener listener;
   
   private final String id;
   
   private volatile boolean started;
   
   private static final ExecutorFactory factory =
      new OrderedExecutorFactory(Executors.newCachedThreadPool(new JBMThreadFactory("JBM-InVM-Transport-Threads")));
   
   private final Executor executor;
         
   public InVMConnection(final BufferHandler handler, final ConnectionLifeCycleListener listener)
   {
      this (UUIDGenerator.getInstance().generateSimpleStringUUID().toString(), handler, listener);
   }
   
   public InVMConnection(final String id, final BufferHandler handler, final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;
      
      this.listener = listener;
      
      this.id = id;
      
      this.executor = factory.getExecutor();
      
      listener.connectionCreated(this);
      
      started = true;
   }

   public void close()
   {      
      if (!started)
      {
         return;
      }
      
      //Wait for writes to be processed
      Future future = new Future();
      
      executor.execute(future);
      
      boolean ok = future.await(10000);
      
      if (!ok)
      {
         log.warn("Timed out waiting for connection writes to be processed");
      }
       
      listener.connectionDestroyed(id);
      
      started = false;
   }

   public MessagingBuffer createBuffer(int size)
   {
      return new ByteBufferWrapper(ByteBuffer.allocate(size));
   }

   public Object getID()
   {
      return id;
   }

   public void write(final MessagingBuffer buffer)
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               buffer.getInt(); //read and discard
               handler.bufferReceived(id, buffer);
            }
            catch (Exception e)
            {
               final String msg = "Failed to write to handler";
               log.error(msg, e);
               throw new IllegalStateException(msg, e);         
            }
         }
      });      
   }
}
