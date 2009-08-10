/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.CloseListener;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleIDGenerator;

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

   private volatile boolean active;

   private final boolean client;

   // Channels 0-9 are reserved for the system
   // 0 is for pinging
   // 1 is for session creation and attachment
   // 2 is for replication
   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(10);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   private boolean frozen;

   private final Object failLock = new Object();
   
   private final PacketDecoder decoder = new PacketDecoder();
   
   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final List<Interceptor> interceptors)
   {
      this(transportConnection, blockingCallTimeout, interceptors, true, true);
   }

   /*
    * Create a server side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final List<Interceptor> interceptors,
                                 final boolean active)

   {
      this(transportConnection, -1, interceptors, active, false);
   }

   private RemotingConnectionImpl(final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final List<Interceptor> interceptors,
                                  final boolean active,
                                  final boolean client)

   {
      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;

      this.interceptors = interceptors;

      this.active = active;

      this.client = client;
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

   public MessagingBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final MessagingException me)
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
   
   public boolean isActive()
   {
      return active;
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

   // Buffer Handler implementation
   // ----------------------------------------------------

   public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
   {
      final Packet packet = decoder.decode(buffer);

      synchronized (transferLock)
      {
         if (!frozen)
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

            final Channel channel = channels.get(packet.getChannelID());

            if (channel != null)
            {
               channel.handlePacket(packet);
            }
         }
      }
   }

   public void activate()
   {
      active = true;
   }

   public void freeze()
   {
      // Prevent any more packets being handled on this connection

      synchronized (transferLock)
      {
         frozen = true;
      }
   }

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void callFailureListeners(final MessagingException me)
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
