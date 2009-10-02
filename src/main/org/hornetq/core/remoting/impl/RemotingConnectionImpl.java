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

package org.hornetq.core.remoting.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleIDGenerator;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class RemotingConnectionImpl extends AbstractBufferHandler implements RemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Connection transportConnection;

   private final Map<Long, Channel> channels = new ConcurrentHashMap<Long, Channel>();

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private final long blockingCallTimeout;

   private final List<Interceptor> interceptors;

   private volatile boolean destroyed;

   private final boolean client;

   // Channels 0-9 are reserved for the system
   // 0 is for pinging
   // 1 is for session creation and attachment
   // 2 is for replication
   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(10);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   // private boolean frozen;

   private final Object failLock = new Object();

   private final PacketDecoder decoder = new PacketDecoder();

   private volatile boolean dataReceived;

   private final Executor executor;

   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final List<Interceptor> interceptors)
   {
      this(transportConnection, blockingCallTimeout, interceptors, true, null);
   }

   /*
    * Create a server side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final List<Interceptor> interceptors,
                                 final Executor executor)

   {
      this(transportConnection, -1, interceptors, false, executor);
   }

   private RemotingConnectionImpl(final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final List<Interceptor> interceptors,
                                  final boolean client,
                                  final Executor executor)

   {
      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;

      this.interceptors = interceptors;

      this.client = client;

      this.executor = executor;
   }

   // RemotingConnection implementation
   // ------------------------------------------------------------

   public Connection getTransportConnection()
   {
      return this.transportConnection;
   }

   public List<FailureListener> getFailureListeners()
   {
      return new ArrayList<FailureListener>(failureListeners);
   }

   public void setFailureListeners(final List<FailureListener> listeners)
   {
      this.failureListeners.clear();

      this.failureListeners.addAll(listeners);
   }

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   public synchronized Channel getChannel(final long channelID, final int windowSize, final boolean block)
   {
      Channel channel = channels.get(channelID);

      if (channel == null)
      {
         channel = new ChannelImpl(this, channelID, windowSize, block);

         channels.put(channelID, channel);
      }

      return channel;
   }

   public synchronized boolean removeChannel(final long channelID)
   {
      return channels.remove(channelID) != null;
   }

   public synchronized void putChannel(final long channelID, final Channel channel)
   {
      channels.put(channelID, channel);
   }

   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   public void addCloseListener(CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final HornetQException me)
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }

      log.warn("Connection failure has been detected: " + me.getMessage() + " [code=" + me.getCode() + "]");

      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      internalClose();

      for (Channel channel : channels.values())
      {
         channel.returnBlocking();
      }
   }

   public void destroy()
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }

      internalClose();

      callClosingListeners();
   }

   public long generateChannelID()
   {
      return idGenerator.generateID();
   }

   public synchronized void syncIDGeneratorSequence(final long id)
   {
      if (!idGeneratorSynced)
      {
         idGenerator = new SimpleIDGenerator(id);

         idGeneratorSynced = true;
      }
   }

   public long getIDGeneratorSequence()
   {
      return idGenerator.getCurrentID();
   }

   public Object getTransferLock()
   {
      return transferLock;
   }

   public boolean isClient()
   {
      return client;
   }

   public boolean isDestroyed()
   {
      return destroyed;
   }

   public long getBlockingCallTimeout()
   {
      return blockingCallTimeout;
   }

   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   // Buffer Handler implementation
   // ----------------------------------------------------

   public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
   {
      final Packet packet = decoder.decode(buffer);
      
      if (executor == null || packet.getType() == PacketImpl.PING)
      {
         // Pings must always be handled out of band so we can send pings back to the client quickly
         // otherwise they would get in the queue with everything else which might give an intolerable delay
         doBufferReceived(packet);
      }
      else
      {
         executor.execute(new Runnable()
         {
            public void run()
            {
               doBufferReceived(packet);
            }
         });
      }

      dataReceived = true;
   }

   private void doBufferReceived(final Packet packet)
   {
      if (interceptors != null)
      {            
         for (final Interceptor interceptor : interceptors)
         {
            try
            {
               boolean callNext = interceptor.intercept(packet, this);

               if (!callNext)
               {
                  // abort

                  return;
               }
            }
            catch (final Throwable e)
            {
               log.warn("Failure in calling interceptor: " + interceptor, e);
            }
         }
      }
      
      synchronized (transferLock)
      {         
         final Channel channel = channels.get(packet.getChannelID());

         if (channel != null)
         {
            channel.handlePacket(packet);
         }                 
      }
   }

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void callFailureListeners(final HornetQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   private void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone)
      {
         try
         {
            listener.connectionClosed();
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   private void internalClose()
   {
      // We close the underlying transport connection
      transportConnection.close();

      for (Channel channel : channels.values())
      {
         channel.close();
      }
   }

}
