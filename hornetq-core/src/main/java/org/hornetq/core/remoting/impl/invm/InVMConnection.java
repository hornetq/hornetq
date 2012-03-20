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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.utils.UUIDGenerator;

/**
 * A InVMConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMConnection implements Connection
{
   private static final boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final String id;

   private boolean closed;
   
   // Used on tests
   public static boolean flushEnabled = true;

   private final int serverID;

   private final Executor executor;
   
   private volatile boolean closing;

   private HornetQPrincipal defaultHornetQPrincipal;

   public InVMConnection(final Acceptor acceptor, 
                         final int serverID,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this(acceptor, serverID, UUIDGenerator.getInstance().generateSimpleStringUUID().toString(), handler, listener, executor);
   }

   public InVMConnection(final Acceptor acceptor, 
                         final int serverID,
                         final String id,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this(acceptor, serverID, id, handler, listener, executor, null);
   }

   public InVMConnection(final Acceptor acceptor,
                         final int serverID,
                         final String id,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor,
                         HornetQPrincipal defaultHornetQPrincipal)
   {
      this.serverID = serverID;

      this.handler = handler;

      this.listener = listener;

      this.id = id;

      this.executor = executor;

      this.defaultHornetQPrincipal = defaultHornetQPrincipal;

      listener.connectionCreated(acceptor, this, ProtocolType.CORE);
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
                     if (isTrace)
                     {
                        HornetQLogger.LOGGER.trace(InVMConnection.this + "::Sending inVM packet");
                     }
                     handler.bufferReceived(id, copied);
                  }
               }
               catch (Exception e)
               {
                  final String msg = "Failed to write to handler on connector " + this;
                  HornetQLogger.LOGGER.errorWritingToInvmConnector(e, this);
                  throw new IllegalStateException(msg, e);
               }
               finally
               {
                  if (isTrace)
                  {
                     HornetQLogger.LOGGER.trace(InVMConnection.this + "::packet sent done");
                  }
               }
            }
         });
         
         if (flush && flushEnabled)
         {
            final CountDownLatch latch = new CountDownLatch(1);
            executor.execute(new Runnable(){
               public void run()
               {
                  latch.countDown();
               }
            });
            
            try
            {
               if (!latch.await(10, TimeUnit.SECONDS))
               {
                  HornetQLogger.LOGGER.timedOutFlushingInvmChannel();
               }
            }
            catch (InterruptedException e)
            {
               HornetQLogger.LOGGER.errorflushingInvmChannel(e);
            }
         }
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
   
   public void addReadyListener(ReadyListener listener)
   {
   }

   public void removeReadyListener(ReadyListener listener)
   {
   }

   public HornetQPrincipal getDefaultHornetQPrincipal()
   {
      return defaultHornetQPrincipal;
   }

   public void disableFlush()
   {
      flushEnabled = false;
   }
   
   public Executor getExecutor()
   {
      return executor;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "InVMConnection [serverID=" + serverID + ", id=" + id + "]";
   }
   
   
}
