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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.logging.Logger;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
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
   
   private volatile boolean closing;

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

      listener.connectionCreated(this, ProtocolType.CORE);
   }

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
      return HornetQBuffers.dynamicBuffer(size);
   }

   public Object getID()
   {
      return id;
   }
   
   public void checkFlushBatchBuffer()
   {
   }

   public void write(final HornetQBuffer buffer)
   {
      write(buffer, false, false);
   }
   
   public void write(final HornetQBuffer buffer, final boolean flush, final boolean batch)
   {
      final HornetQBuffer copied = buffer.copy(0, buffer.capacity());

      copied.setIndex(buffer.readerIndex(), buffer.writerIndex());

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
                     copied.readInt(); // read and discard

                     handler.bufferReceived(id, copied);
                  }
               }
               catch (Exception e)
               {
                  final String msg = "Failed to write to handler";
                  InVMConnection.log.error(msg, e);
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
   
   public int getBatchingBufferSize()
   {
      return -1;
   }

}
