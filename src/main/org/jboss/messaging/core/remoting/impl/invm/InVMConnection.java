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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.Future;
import org.jboss.messaging.utils.UUIDGenerator;

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

   private boolean closed;

   private final int serverID;

   private final Executor executor;

   public InVMConnection(final int serverID,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this(serverID, UUIDGenerator.getInstance().generateSimpleStringUUID().toString(), handler, listener, executor);
   }

   public InVMConnection(final int serverID,
                         final String id,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this.serverID = serverID;

      this.handler = handler;

      this.listener = listener;

      this.id = id;

      this.executor = executor;

      listener.connectionCreated(this);
   }

   private volatile boolean closing;
   
   public void close()
   {      
      if (closing)
      {
         return;
      }
      
      closing = true;

      synchronized (this)
      {         
         // Must execute this on the executor, to ensure connection destroyed doesn't get fired before the last DISCONNECT
         // packet is processed   
         try
         {
            executor.execute(new Runnable()
            {
               public void run()
               {
                  if (!closed)
                  {
//                     log.info("calling listener connection destroyed: " + listener);
                     listener.connectionDestroyed(id);
   
                     closed = true;
                  }
               }
            });
            
            Future future = new Future();
            
            executor.execute(future);
            
            boolean ok = future.await(10000);
            
            if (!ok)
            {
               log.warn("Timed out waiting to close");
            }
         }
         catch (RejectedExecutionException e)
         {
            // Ignore - this can happen if server/client is shutdown
         }
      }
   }

   public MessagingBuffer createBuffer(final int size)
   {
      return ChannelBuffers.buffer(size);
   }

   public Object getID()
   {
      return id;
   }

   public void write(final MessagingBuffer buffer)
   {
      write(buffer, false);
   }

   public void write(final MessagingBuffer buffer, final boolean flush)
   {
      try
      {
         executor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  if (!closed)
                  {
                     buffer.readInt(); // read and discard

                     handler.bufferReceived(id, buffer);
                  }
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
      catch (RejectedExecutionException e)
      {
         // Ignore - this can happen if server/client is shutdown and another request comes in
      }
   }

   public String getRemoteAddress()
   {
      return "invm:" + serverID;
   }

   public void fail(final MessagingException me)
   {
      listener.connectionException(id, me);
   }

}
