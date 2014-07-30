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

package org.hornetq.core.remoting.impl.netty;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.utils.ConcurrentHashSet;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyConnection implements Connection
{
   // Constants -----------------------------------------------------
   private static final int BATCHING_BUFFER_SIZE = 8192;

   // Attributes ----------------------------------------------------

   private final Channel channel;

   private boolean closed;

   private final ConnectionLifeCycleListener listener;

   private final boolean batchingEnabled;

   private final boolean directDeliver;

   private volatile HornetQBuffer batchBuffer;

   private final Map<String, Object> configuration;

   private final Semaphore writeLock = new Semaphore(1);

   private final Set<ReadyListener> readyListeners = new ConcurrentHashSet<ReadyListener>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public NettyConnection(final Map<String, Object> configuration,
                           final Channel channel,
                           final ConnectionLifeCycleListener listener,
                           boolean batchingEnabled,
                           boolean directDeliver)
   {
      this.configuration = configuration;

      this.channel = channel;

      this.listener = listener;

      this.batchingEnabled = batchingEnabled;

      this.directDeliver = directDeliver;
   }

   // Public --------------------------------------------------------

   public Channel getNettyChannel()
   {
      return channel;
   }
   // Connection implementation ----------------------------


   public void forceClose()
   {
      if (channel != null)
      {
         try
         {
            channel.close();
         }
         catch (Throwable e)
         {
            HornetQClientLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   public synchronized void close()
   {
      if (closed)
      {
         return;
      }

      SslHandler sslHandler = (SslHandler)channel.getPipeline().get("ssl");
      if (sslHandler != null)
      {
         try
         {
            ChannelFuture sslCloseFuture = sslHandler.close();

            if (!sslCloseFuture.awaitUninterruptibly(10000))
            {
               HornetQClientLogger.LOGGER.timeoutClosingSSL();
            }
         }
         catch (Throwable t)
         {
            // ignore
         }
      }

      ChannelFuture closeFuture = channel.close();

      if (!closeFuture.awaitUninterruptibly(10000))
      {
         HornetQClientLogger.LOGGER.timeoutClosingNettyChannel();
      }

      closed = true;

      listener.connectionDestroyed(getID());
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return new ChannelBufferWrapper(ChannelBuffers.dynamicBuffer(size));
   }

   public Object getID()
   {
      return channel.getId();
   }

   // This is called periodically to flush the batch buffer
   public void checkFlushBatchBuffer()
   {
      if (!batchingEnabled)
      {
         return;
      }

      if (writeLock.tryAcquire())
      {
         try
         {
            if (batchBuffer != null && batchBuffer.readable())
            {
               channel.write(batchBuffer.channelBuffer());

               batchBuffer = HornetQBuffers.dynamicBuffer(BATCHING_BUFFER_SIZE);
            }
         }
         finally
         {
            writeLock.release();
         }
      }
   }

   public void write(final HornetQBuffer buffer)
   {
      write(buffer, false, false);
   }

   public void write(HornetQBuffer buffer, final boolean flush, final boolean batched)
   {

      try
      {
         writeLock.acquire();

         try
         {
            if (batchBuffer == null && batchingEnabled && batched && !flush)
            {
               // Lazily create batch buffer

               batchBuffer = HornetQBuffers.dynamicBuffer(BATCHING_BUFFER_SIZE);
            }

            if (batchBuffer != null)
            {
               batchBuffer.writeBytes(buffer, 0, buffer.writerIndex());

               if (batchBuffer.writerIndex() >= BATCHING_BUFFER_SIZE || !batched || flush)
               {
                  // If the batch buffer is full or it's flush param or not batched then flush the buffer

                  buffer = batchBuffer;
               }
               else
               {
                  return;
               }

               if (!batched || flush)
               {
                  batchBuffer = null;
               }
               else
               {
                  // Create a new buffer

                  batchBuffer = HornetQBuffers.dynamicBuffer(BATCHING_BUFFER_SIZE);
               }
            }

            ChannelFuture future = channel.write(buffer.channelBuffer());

            if (flush)
            {
               while (true)
               {
                  try
                  {
                     boolean ok = future.await(10000);

                     if (!ok)
                     {
                        HornetQClientLogger.LOGGER.timeoutFlushingPacket();
                     }

                     break;
                  }
                  catch (InterruptedException e)
                  {
                     throw new HornetQInterruptedException(e);
                  }
               }
            }
         }
         finally
         {
            writeLock.release();
         }
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
      }
   }

   public String getRemoteAddress()
   {
      return channel.getRemoteAddress().toString();
   }

   public boolean isDirectDeliver()
   {
      return directDeliver;
   }

   public void addReadyListener(final ReadyListener listener)
   {
      readyListeners.add(listener);
   }

   public void removeReadyListener(final ReadyListener listener)
   {
      readyListeners.remove(listener);
   }

   //never allow this
   public HornetQPrincipal getDefaultHornetQPrincipal()
   {
      return null;
   }

   void fireReady(final boolean ready)
   {
      for (ReadyListener listener: readyListeners)
      {
         listener.readyForWriting(ready);
      }
   }


   @Override
   public TransportConfiguration getConnectorConfig()
   {
      if (configuration != null)
      {
         return new TransportConfiguration(NettyConnectorFactory.class.getName(), this.configuration);
      }
      else
      {
         return null;
      }
   }


   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return super.toString() + "[local= " + channel.getLocalAddress() + ", remote=" + channel.getRemoteAddress() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
