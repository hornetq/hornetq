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

package org.hornetq.core.protocol.core.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.SimpleIDGenerator;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class RemotingConnectionImpl implements BufferHandler, CoreRemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

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

   private int clientVersion;

   // Channels 0-9 are reserved for the system
   // 0 is for pinging
   // 1 is for session creation and attachment
   // 2 is for replication
   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(10);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   private final Object failLock = new Object();

   private final PacketDecoder decoder = new PacketDecoder();

   private volatile boolean dataReceived;

   private final Executor executor;

   private volatile boolean executing;

   private final SimpleString nodeID;

   private final long creationTime;

   private String clientID;

   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final List<Interceptor> interceptors)
   {
      this(transportConnection, blockingCallTimeout, interceptors, true, null, null);
   }

   /*
    * Create a server side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final List<Interceptor> interceptors,
                                 final Executor executor,
                                 final SimpleString nodeID)

   {
      this(transportConnection, -1, interceptors, false, executor, nodeID);
   }

   private RemotingConnectionImpl(final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final List<Interceptor> interceptors,
                                  final boolean client,
                                  final Executor executor,
                                  final SimpleString nodeID)

   {
      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;

      this.interceptors = interceptors;

      this.client = client;

      this.executor = executor;

      this.nodeID = nodeID;

      this.creationTime = System.currentTimeMillis();
   }




   // RemotingConnection implementation
   // ------------------------------------------------------------

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "RemotingConnectionImpl [clientID=" + clientID +
             ", nodeID=" +
             nodeID +
             ", transportConnection=" +
             transportConnection +
             "]";
   }

   public Connection getTransportConnection()
   {
      return transportConnection;
   }

   public List<FailureListener> getFailureListeners()
   {
      return new ArrayList<FailureListener>(failureListeners);
   }

   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   /**
    * @return the clientVersion
    */
   public int getClientVersion()
   {
      return clientVersion;
   }

   /**
    * @param clientVersion the clientVersion to set
    */
   public void setClientVersion(int clientVersion)
   {
      this.clientVersion = clientVersion;
   }

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   public synchronized Channel getChannel(final long channelID, final int confWindowSize)
   {
      Channel channel = channels.get(channelID);

      if (channel == null)
      {
         channel = new ChannelImpl(this, channelID, confWindowSize);

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

   public void addCloseListener(final CloseListener listener)
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

   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
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

      RemotingConnectionImpl.log.warn("Connection failure has been detected: " + me.getMessage() +
                                      " [code=" +
                                      me.getCode() +
                                      "]");

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

   public void disconnect(final boolean criticalError)
   {
      Channel channel0 = getChannel(0, -1);

      // And we remove all channels from the connection, this ensures no more packets will be processed after this
      // method is
      // complete

      Set<Channel> allChannels = new HashSet<Channel>(channels.values());

      if (!criticalError)
      {
         removeAllChannels();
      }
      else
      {
         // We can't hold a lock if a critical error is happening...
         // as other threads will be holding the lock while hanging on IO
         channels.clear();
      }

      // Now we are 100% sure that no more packets will be processed we can flush then send the disconnect

      if (!criticalError)
      {
         for (Channel channel: allChannels)
         {
            channel.flushConfirmations();
         }
      }

      Packet disconnect = new DisconnectMessage(nodeID);
      channel0.sendAndFlush(disconnect);
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

   //We flush any confirmations on the connection - this prevents idle bridges for example
   //sitting there with many unacked messages
   public void flush()
   {
      synchronized (transferLock)
      {
         for (Channel channel : channels.values())
         {
            channel.flushConfirmations();
         }
      }
   }

   public void checkFlushBatchBuffer()
   {
      transportConnection.checkFlushBatchBuffer();
   }

   // Buffer Handler implementation
   // ----------------------------------------------------

   public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
   {
      try
      {
         final Packet packet = decoder.decode(buffer);

         if (isTrace)
         {
            log.trace("handling packet " + packet);
         }

         if (packet.isAsyncExec() && executor != null)
         {
            executing = true;

            executor.execute(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doBufferReceived(packet);
                  }
                  catch (Throwable t)
                  {
                     RemotingConnectionImpl.log.error("Unexpected error", t);
                  }

                  executing = false;
               }
            });
         }
         else
         {
            //To prevent out of order execution if interleaving sync and async operations on same connection
            while (executing)
            {
               Thread.yield();
            }

            // Pings must always be handled out of band so we can send pings back to the client quickly
            // otherwise they would get in the queue with everything else which might give an intolerable delay
            doBufferReceived(packet);
         }

         dataReceived = true;
      }
      catch (Exception e)
      {
         log.error("Failed to decode", e);
      }
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
                  return;
               }
            }
            catch (final Throwable e)
            {
               RemotingConnectionImpl.log.warn("Failure in calling interceptor: " + interceptor, e);
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

   private void removeAllChannels()
   {
      // We get the transfer lock first - this ensures no packets are being processed AND
      // it's guaranteed no more packets will be processed once this method is complete
      synchronized (transferLock)
      {
         channels.clear();
      }
   }

   private void callFailureListeners(final HornetQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false);
         }
         catch (HornetQInterruptedException interrupted)
         {
            // this is an expected behaviour.. no warn or error here
            log.debug("thread interrupted", interrupted);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            RemotingConnectionImpl.log.error("Failed to execute failure listener", t);
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
            RemotingConnectionImpl.log.error("Failed to execute failure listener", t);
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

   public void setClientID(String cID)
   {
      clientID = cID;
   }

   public String getClientID()
   {
      return clientID;
   }
}
