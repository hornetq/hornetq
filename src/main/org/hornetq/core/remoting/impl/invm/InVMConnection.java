/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.remoting.impl.invm;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.UUIDGenerator;

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
         if (!closed)
         {
            listener.connectionDestroyed(id);

            closed = true;
         }
      }
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return ChannelBuffers.buffer(size);
   }

   public Object getID()
   {
      return id;
   }

   public void write(final HornetQBuffer buffer)
   {
      write(buffer, false);
   }

   public void write(final HornetQBuffer buffer, final boolean flush)
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

   public void fail(final HornetQException me)
   {
      listener.connectionException(id, me);
   }

}
